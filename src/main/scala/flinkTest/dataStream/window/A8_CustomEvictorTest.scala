package flinkTest.dataStream.window

import java.lang

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/10/11 20:06:06
 * @Description: A8_CustomEvictorTest  自定义移除器 移除元素
 * @Version 1.0.0
 */
object A8_CustomEvictorTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val keyedStream: KeyedStream[(String, Int), String] = inputStream.map((_, 1)).keyBy(_._1)

    val windowStream: DataStream[(String, Int)] = keyedStream
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .evictor(CustomEvictor(true))
      .apply(new WindowFunction[(String, Int), (String, Int), String, TimeWindow] {
        override def apply(key: String, window: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
          var outKey: String = null
          var sum: Int = 0
          for (elem <- input) {
            outKey = elem._1
            sum += elem._2
          }
          out.collect(outKey, sum)
        }
      })

    windowStream.filter(_._1 != null).print()
    env.executeAsync("A8_CustomEvictorTest")
  }

  case class CustomEvictor(isBefore: Boolean) extends Evictor[(String, Int), TimeWindow] {

    //将 aa 数据剔出 不会参与计算。输出 (null,0)
    override def evictBefore(elements: lang.Iterable[TimestampedValue[(String, Int)]], size: Int, window: TimeWindow, evictorContext: Evictor.EvictorContext): Unit = {
      if (isBefore) {
        evictor(elements, size, window, evictorContext)
      }
    }

    override def evictAfter(elements: lang.Iterable[TimestampedValue[(String, Int)]], size: Int, window: TimeWindow, evictorContext: Evictor.EvictorContext): Unit = {
      if (!isBefore) {
        evictor(elements, size, window, evictorContext)
      }
    }

    def evictor(elements: lang.Iterable[TimestampedValue[(String, Int)]], size: Int, window: TimeWindow, evictorContext: Evictor.EvictorContext) = {
      val iterator = elements.iterator()
      while (iterator.hasNext) {
        val it = iterator.next()
        if (it.getValue._1 == ("aa")) {
          iterator.remove()
        }
      }
    }

  }

}
