package flinkTest.dataStream.t1_operators

import java.text.SimpleDateFormat
import java.util.Date

import flinkTest.dataStream.t1_operators.A5_ReduceTest.filterFuntion
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/10/9 16:43:38
 * @Description: A7_WindowApplyTest
 *               apply 全窗口函数 可以获取到窗口信息(窗口起始时间，结束时间。。。)
 * @Version 1.0.0
 */
object A7_WindowApplyTest {
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

    // 2 秒一个窗口
    val windowStream: DataStream[(String, String, String, Int)] = keyedStream
      .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
      // 使用 这种窗口函数 可以获取到窗口信息
      .apply(new WindowFunction[(String, Int), (String, String, String, Int), String, TimeWindow] {
        override def apply(key: String, window: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, String, String, Int)]): Unit = {
          val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")
          var sum: Int = 0
          for (elem <- input) {
            sum += elem._2
          }
          val start: Date = new Date(window.getStart)
          val startWindow: String = dateFormat.format(start)
          val end: Date = new Date(window.getEnd)
          val endWindow: String = dateFormat.format(end)
          // 输出
          out.collect((key, startWindow, endWindow, sum))
        }
      })

    windowStream.print()
    env.executeAsync("A6_WindowTest")
  }

}
