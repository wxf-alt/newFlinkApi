package flinkTest.dataStream.t1_operators

import flinkTest.dataStream.t1_operators.A5_ReduceTest.filterFuntion
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Auther: wxf
 * @Date: 2022/10/9 16:28:38
 * @Description: A6_Window_Test
 *               Window 使用在 KeyedStream 上
 *               WindowAll 使用在 DataStream 上  用法一样
 * @Version 1.0.0
 */
object A6_WindowAndWindowAllTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val mapStream: DataStream[(String, Int)] = inputStream
      .filter(filterFuntion(_))
      .map(x => {
        val str: Array[String] = x.split(" ")
        (str(0), str(1).toInt)
      })

    // 使用 Window
    val keyedStream: KeyedStream[(String, Int), String] = mapStream.keyBy(_._1)

    // 5 秒一个窗口
    val windowStream: DataStream[(String, Int)] = keyedStream
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .reduce((x, y) => (x._1, x._2 + y._2))

    //    // 使用 WindowAll
    //    val windowStream: DataStream[(String, Int)] = mapStream
    //      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    //      .reduce((x, y) => (x._1, x._2 + y._2))

    windowStream.print()
    env.executeAsync("A6_WindowTest")
  }
}
