package flinkTest.dataStream.operators

import flinkTest.dataStream.t1_operators.A5_ReduceTest.filterFuntion
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.functions.co.{CoMapFunction, RichCoMapFunction}
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/10/10 16:59:51
 * @Description: A12_ConnectTest  connect 允许在两个流的处理逻辑之间共享状态
 *              设置 keyedState 统计两个流中 相同key值 有多少数据
 * @Version 1.0.0
 */
object A12_ConnectTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val inputStream1: DataStream[String] = env.socketTextStream("localhost", 6666)
    val inputStream2: DataStream[String] = env.socketTextStream("localhost", 7777)

    val mapStream1: DataStream[(String, Int)] = inputStream1
      .filter(filterFuntion(_))
      .map(x => {
        val str: Array[String] = x.split(" ")
        (str(0), str(1).toInt)
      })

    val mapStream2: DataStream[(String, String)] = inputStream2
      .map(x => {
        val str: Array[String] = x.split(" ")
        (str(0), str(1))
      })

    val connectedStream: ConnectedStreams[(String, Int), (String, String)] = mapStream1.connect(mapStream2).keyBy(x => x._1, y => y._1)

    val coMapStream: DataStream[(String, String, Int)] = connectedStream.map(new RichCoMapFunction[(String, Int), (String, String), (String, String, Int)] {
      lazy val counter: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("counter", classOf[Int], 1))

      override def map1(value: (String, Int)) = {
        val count: Int = counter.value()
        counter.update(count + 1)
        (value._1, value._2.toString, count)
      }

      override def map2(value: (String, String)) = {
        val count: Int = counter.value()
        counter.update(count + 1)
        (value._1, value._2.toString, count)
      }
    })

    coMapStream.print()
    env.executeAsync("A12_ConnectTest")
  }
}
