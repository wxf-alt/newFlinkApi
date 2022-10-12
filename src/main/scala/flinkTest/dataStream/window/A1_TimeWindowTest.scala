package flinkTest.dataStream.window

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Auther: wxf
 * @Date: 2022/10/11 15:20:19
 * @Description: A1_TimeWindowTest   滚动窗口 和 滑动窗口 实现一样
 * @Version 1.0.0
 */
object A1_TimeWindowTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val keyedStream: KeyedStream[(String, Int), String] = inputStream.map((_, 1)).keyBy(_._1)

    // 使用 Processing 时间;如果想使用 Event 时间语义 需要设置 WaterMark 和 指定时间字段
//    val windowStream: DataStream[(String, Int)] = keyedStream
//      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//      .reduce((x, y) => (x._1, x._2 + y._2))

    val windowStream: DataStream[(String, Int)] = keyedStream
      .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))
      .reduce((x, y) => (x._1, x._2 + y._2))

    windowStream.print()
    env.executeAsync("A1_TumblingWindow")
  }
}
