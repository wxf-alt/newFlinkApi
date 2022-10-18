package flinkTest.dataStream.state_checkpoint

import bean.Sensor
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.StateTtlConfig.{StateVisibility, UpdateType}
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/9/14 17:38:42
 * @Description: a7_keyedStateScalaApiTest  状态有效期设置
 * @Version 1.0.0
 */
object a6_KeyedStateTTLTest extends App {
  val conf: Configuration = new Configuration()
  conf.setInteger(RestOptions.PORT, 8081)
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
  env.setParallelism(1)

  val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)

  val mapStream: DataStream[Sensor] = inputStream.map(x => {
    val str: Array[String] = x.split(" ")
    Sensor(str(0), str(1).toLong * 1000, str(2).toDouble)
  })

  // 建议 使用 KeySelector 的方式,指定key
  val keyStream: KeyedStream[Sensor, String] = mapStream.keyBy(new KeySelector[Sensor, String] {
    override def getKey(value: Sensor): String = value.id
  })

  val flatMapStream: DataStream[(String, Long, Double)] = keyStream.flatMap(new CountWindowAverageTtl())

  flatMapStream.print("flatMapStream：")

  env.executeAsync("a7_keyedStateScalaApiTest")

}

class CountWindowAverageTtl extends RichFlatMapFunction[Sensor, (String, Long, Double)] {

  // 创建 值状态
  var sum: ValueState[(Long, Double)] = _

  override def open(parameters: Configuration): Unit = {
    val valueStateDescriptor: ValueStateDescriptor[(Long, Double)] = new ValueStateDescriptor[(Long, Double)]("average", createTypeInformation[(Long, Double)])
    val stateTtlConfig: StateTtlConfig = StateTtlConfig
      .newBuilder(Time.minutes(2))
      .setUpdateType(UpdateType.OnCreateAndWrite) // 在创建和写入时更新(默认)
      .setStateVisibility(StateVisibility.NeverReturnExpired) // 从不返回过期数据(默认)
      .disableCleanupInBackground() // 关闭后台清理 在后台禁用过期状态的默认清理(默认启用)
      .cleanupIncrementally(10, true)
      .build()
    valueStateDescriptor.enableTimeToLive(stateTtlConfig)
    sum = getRuntimeContext.getState(valueStateDescriptor)

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
    // 状态是 实时更新的 下面的 newSum 也可以使用 sum状态 进行计算。
    //    因为上面已经将newSum的值更新到状态 sum 中
    if (newSum._1 >= 2) {
      out.collect((value.id, newSum._1, newSum._2 / newSum._1))
      sum.clear()
    }
  }

}
