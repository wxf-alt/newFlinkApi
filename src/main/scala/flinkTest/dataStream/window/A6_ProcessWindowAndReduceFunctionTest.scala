package flinkTest.dataStream.window


import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/10/11 19:11:26
 * @Description: A6_ProcessWindowAndReduceFunctionTest
 *               增量聚合的 ProcessWindowFunction
 *               ReduceFunction 与 ProcessWindowFunction 组合，返回窗口中的最小元素和窗口的开始时间。
 * @Version 1.0.0
 */
object A6_ProcessWindowAndReduceFunctionTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val keyedStream: KeyedStream[(String, Int), String] = inputStream
      .map(x => {
        val str: Array[String] = x.split(" ")
        (str(0), str(1).toInt)
      })
      .keyBy(_._1)

    val windowStream: DataStream[(Long, String, Int)] = keyedStream
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .reduce(reduceFunctionAndProcessFunction(), processFunctionAndReduceFunction())

    windowStream.print()
    env.executeAsync("A6_ProcessWindowAndReduceFunctionTest")
  }

  // 返回 窗口中 key的最小值
  case class reduceFunctionAndProcessFunction() extends ReduceFunction[(String, Int)] {
    override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
      (value1._1, Math.min(value1._2, value2._2))
    }
  }

  case class processFunctionAndReduceFunction() extends ProcessWindowFunction[(String, Int), (Long, String, Int), String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(Long, String, Int)]): Unit = {
      val minValue: (String, Int) = elements.iterator.next()
      val start: Long = context.window.getStart
      out.collect((start, key, minValue._2))
    }
  }

}
