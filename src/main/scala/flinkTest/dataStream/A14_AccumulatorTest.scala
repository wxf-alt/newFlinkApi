package flinkTest.dataStream

import bean.Sensor
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.{Accumulator, LongCounter}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/10/8 19:58:18
 * @Description: a14_AccumulatorTest
 * @Version 1.0.0
 */
object a14_AccumulatorTest extends App {
  val conf: Configuration = new Configuration()
  conf.setInteger(RestOptions.PORT, 8081)
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
  env.setParallelism(1)

  // 创建一个累加器对象
  val longCounter: LongCounter = new LongCounter()

  val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)

  val mapStream: DataStream[Sensor] = inputStream.map(x => {
    val str: Array[String] = x.split(" ")
    Sensor(str(0), str(1).toLong * 1000, str(2).toDouble)
  })

  val result: DataStream[(String, Double)] = mapStream
    .keyBy(_.id)
    .map(new MyMapFunction())
  //  val value1: DataStream[(String, Double)] = result.map(new MyMapFunction2(longCounter))
  //  value1.print()

  val jobExecutionResult: JobExecutionResult = env.execute()

  // 查看不了 累加结果；只能去 WebUi 上查看
  val acc: Long = jobExecutionResult.getAccumulatorResult[Long]("num-lines")
  println("acc：" + acc)

}

class MyMapFunction() extends RichMapFunction[Sensor, (String, Double)] {

  //  // 创建一个累加器对象
  private val longCounter: LongCounter = new LongCounter()

  override def open(parameters: Configuration): Unit = {
    // 注册累加器
    getRuntimeContext.addAccumulator("num-lines", longCounter)
  }

  override def map(value: Sensor): (String, Double) = {
    longCounter.add(1)
    (value.id, value.temperature)
  }


}

// 在不同的操作 function 里面使用同一个累加器
class MyMapFunction2(longCounter: LongCounter) extends RichMapFunction[(String, Double), (String, Double)] {

  //  override def open(parameters: Configuration): Unit = {
  //    // 注册累加器
  //    getRuntimeContext.addAccumulator("num-lines", longCounter)
  //  }

  override def map(value: (String, Double)): (String, Double) = {
    longCounter.add(1)
    value
  }


}
