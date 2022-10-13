package flinkTest.dataStream.window

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Auther: wxf
 * @Date: 2022/10/12 14:51:28
 * @Description: A10_WindowAllTest  测试 windowAll 使用的并行度
 * @Version 1.0.0
 */
object A10_WindowAllTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(6)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val mapStream: DataStream[(String, Int)] = inputStream.map((_, 1))

    val windowStream: DataStream[(String, Int)] = mapStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .aggregate(new AggregateFunction[(String, Int), (String, Int), (String, Int)] {
        override def createAccumulator() = ("", 0)

        override def add(value: (String, Int), accumulator: (String, Int)) = (value._1, value._2 + accumulator._2)

        override def getResult(accumulator: (String, Int)) = accumulator

        override def merge(a: (String, Int), b: (String, Int)) = (a._1, a._2 + b._2)
      })

    windowStream.print()
    env.executeAsync("A10_WindowAllTest")
  }
}