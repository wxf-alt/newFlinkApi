package flinkTest.dataStream.window

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{ProcessingTimeSessionWindows, SessionWindowTimeGapExtractor}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Auther: wxf
 * @Date: 2022/10/11 16:00:29
 * @Description: A2_SessionWindowTest  设置动态调节窗口
 * @Version 1.0.0
 */
object A2_SessionWindowTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val keyedStream: KeyedStream[(String, Int), String] = inputStream.map((_, 1)).keyBy(_._1)

    // 5秒 超时会话设置
    //    val windowStream: DataStream[(String, Int)] = keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
    // 设置了动态间隔的 processing-time 会话窗口
    val windowStream: DataStream[(String, Int)] = keyedStream.window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor[(String, Int)] {
      override def extract(element: (String, Int)): Long = if (element._1 == "aa") 5 * 1000 else 2 * 60 * 100
    }))
      .reduce((x, y) => (x._1, x._2 + y._2))

    windowStream.print()
    env.executeAsync("A2_SessionWindowTest")

  }
}
