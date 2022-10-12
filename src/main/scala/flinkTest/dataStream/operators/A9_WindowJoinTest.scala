package flinkTest.dataStream.operators

import flinkTest.dataStream.t1_operators.A5_ReduceTest.filterFuntion
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Auther: wxf
 * @Date: 2022/10/9 17:13:39
 * @Description: A9_WindowJoinTest
 *               根据指定的 key 和窗口 join 两个数据流。
 * @Version 1.0.0
 */
object A9_WindowJoinTest {
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

    val mapStream2: DataStream[(String, Int)] = inputStream2
      .filter(filterFuntion(_))
      .map(x => {
        val str: Array[String] = x.split(" ")
        (str(0), str(1).toInt)
      })

    val joinStream: DataStream[(String, Int)] = mapStream1.join(mapStream2)
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
      .apply(new JoinFunction[(String, Int), (String, Int), (String, Int)] {
        override def join(first: (String, Int), second: (String, Int)) = {
          (first._1, first._2 + second._2)
        }
      })
    //      .apply((x, y) => {
    //        (x._1, x._2 + y._2)
    //      })

    joinStream.print()
    env.executeAsync("A9_WindowJoinTest")
  }
}
