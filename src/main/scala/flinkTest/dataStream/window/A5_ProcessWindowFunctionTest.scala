package flinkTest.dataStream.window

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/10/11 19:00:35
 * @Description: A5_ProcessWindowFunctionTest  ProcessWindowFunction
 * @Version 1.0.0
 */
object A5_ProcessWindowFunctionTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val keyedStream: KeyedStream[(String, Int), String] = inputStream.map((_, 1)).keyBy(_._1)

    val windowStream: DataStream[String] = keyedStream
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .process(MyProcessWindowFunction())

    windowStream.print()
    env.executeAsync("A5_ProcessWindowFunctionTest")
  }

  case class MyProcessWindowFunction() extends ProcessWindowFunction[(String, Int), String, String, TimeWindow] {

    override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
      var count: Long = 0L
      for (in <- elements) {
        count = count + 1
      }
      out.collect(s"Window ${context.window} Key: ${key} count: $count")
    }
  }

}
