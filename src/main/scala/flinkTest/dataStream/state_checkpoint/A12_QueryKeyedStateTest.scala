package flinkTest.dataStream.state_checkpoint

import bean.Sensor
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/9/14 16:33:30
 * @Description: a12_queryKeyedStateTest   状态查询设置
 *               使状态 可以 在外部系统读取
 * @Version 1.0.0
 */
object a12_QueryKeyedStateTest {
  def main(args: Array[String]): Unit = {

    //    val conf: Configuration = new Configuration()
    //    conf.setInteger(RestOptions.PORT, 8081)
    //    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.socketTextStream("nn1.hadoop", 6666)

    val mapStream: DataStream[Sensor] = inputStream.map(x => {
      val str: Array[String] = x.split(" ")
      Sensor(str(0), str(1).toLong * 1000, str(2).toDouble)
    })

    // 建议 使用 KeySelector 的方式,指定key
    val keyStream: KeyedStream[Sensor, String] = mapStream.keyBy(new KeySelector[Sensor, String] {
      override def getKey(value: Sensor): String = value.id
    })
    val flatMapStream: DataStream[(String, Long, Double)] = keyStream.flatMap(new CountWindowAverage2())

    flatMapStream.print("flatMapStream：")

    env.executeAsync("a5_keyedStateTest")

  }
}

class CountWindowAverage2 extends RichFlatMapFunction[Sensor, (String, Long, Double)] {

  // 创建 值状态
  var sum: ValueState[(Long, Double)] = _

  override def open(parameters: Configuration): Unit = {
    val average: ValueStateDescriptor[(Long, Double)] = new ValueStateDescriptor[(Long, Double)]("average", createTypeInformation[(Long, Double)])
    average.setQueryable("query-name")
    sum = getRuntimeContext.getState(average)
  }

  override def flatMap(value: Sensor, out: Collector[(String, Long, Double)]): Unit = {
    // 获取状态
    val tmpCurrentSum: (Long, Double) = sum.value()

    // 第一次使用，赋初始值
    val currentSum: (Long, Double) = if (tmpCurrentSum != null) {
      tmpCurrentSum
    } else {
      (0L, 0D)
    }

    // 进来一条数据 更新状态   不论后面什么逻辑,只要进来一条数据 就更新状态
    val newSum: (Long, Double) = (currentSum._1 + 1, currentSum._2 + value.temperature)
    sum.update(newSum)
    println(sum.value()._1 + "-----" + sum.value()._2)

    // 如果计数达到2，则发出平均值并清除状态
    if (newSum._1 >= 2) {
      out.collect((value.id, newSum._1, newSum._2 / newSum._1))
      sum.clear()
    }
  }

}