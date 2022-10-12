package flinkTest.dataStream.window

import org.apache.flink.api.common.functions.{AbstractRichFunction, AggregateFunction, RichAggregateFunction, RichFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Auther: wxf
 * @Date: 2022/10/11 16:54:12
 * @Description: A4_AggregateWindowFunctionTest  AggregateWindowFunction
 * @Version 1.0.0
 */
object A4_AggregateWindowFunctionTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val keyedStream: KeyedStream[(String, Int), String] = inputStream.map((_, 1)).keyBy(_._1)

    val windowStream: DataStream[(String, Long)] = keyedStream
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .aggregate(MyAggregateFunction())

    windowStream.print()
    env.executeAsync("A4_AggregateWindowFunctionTest")
  }

  case class MyAggregateFunction() extends AggregateFunction[(String, Int), (String, Long), (String, Long)] {


    // 创建 累加器
    override def createAccumulator(): (String, Long) = ("", 0L)

    // 把每一条元素加进累加器
    override def add(value: (String, Int), accumulator: (String, Long)): (String, Long) = {
      (value._1, value._2 + accumulator._2)
    }

    override def getResult(accumulator: (String, Long)): (String, Long) = (accumulator._1, accumulator._2)

    override def merge(a: (String, Long), b: (String, Long)): (String, Long) = (a._1, a._2 + b._2)

  }

}
