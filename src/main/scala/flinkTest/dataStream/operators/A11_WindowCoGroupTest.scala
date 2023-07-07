package flinkTest.dataStream.operators

import java.{lang, util}

import flinkTest.dataStream.t1_operators.A5_ReduceTest.filterFuntion
import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

/**
 * @Auther: wxf
 * @Date: 2022/10/10 14:34:07
 * @Description: A11_WindowCoGroupTest  模拟 左/右 连接
 * @Version 1.0.0
 */
object A11_WindowCoGroupTest {
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

    val coGroupStream: DataStream[String] = mapStream1.coGroup(mapStream2)
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .trigger(CountTrigger.of(2)) // 窗口数据达到 个数 就会触发计算。如果一个流连续来了两个。那么会漏掉数据
      .apply(new CoGroupFunction[(String, Int), (String, Int), String] {
        override def coGroup(first: lang.Iterable[(String, Int)], second: lang.Iterable[(String, Int)], out: Collector[String]) = {

          import scala.collection.convert.ImplicitConversions._

          var key: String = null
          val v1: util.Iterator[(String, Int)] = first.iterator()
          val v2: util.Iterator[(String, Int)] = second.iterator()
          while (v1.hasNext && v2.hasNext) {
            val value1: (String, Int) = v1.next
            val value2: (String, Int) = v2.next
            key = value1._1
            val result: String = s"${key}:(first -> iterable:(${value1}),(second -> iterable:(${value2})"
            out.collect(result)
          }
          while (v1.hasNext) {
            val value1: (String, Int) = v1.next
            key = value1._1
            val result: String = s"${key}:(first -> iterable:(${value1}),(second -> iterable:()"
            out.collect(result)
          }
          while (v2.hasNext) {
            val value2: (String, Int) = v2.next
            key = value2._1
            val result: String = s"${key}:(first -> iterable:(),(second -> iterable:(${value2})"
            out.collect(result)
          }

          //          val leftList: List[Int] = List[Int]()
          //          val rightList: List[Int] = List[Int]()
          //          if (first.size != 0 && second.size != 0) {
          //            val firstMap: Map[String, Int] = first.toMap
          //            val secondMap: Map[String, Int] = second.toMap
          //            key = firstMap.keySet.head
          //            result = s"${key}:(first -> iterable:(${firstMap.values.toBuffer}),(second -> iterable:(${secondMap.values.toBuffer})"
          //          } else if (first.size != 0) {
          //            val firstMap: Map[String, Int] = first.toMap
          //            key = firstMap.keySet.head
          //            result = s"${key}:(first -> iterable:(${firstMap.values.toBuffer}),(second -> iterable:(${ArrayBuffer()})"
          //          } else if (second.size != 0) {
          //            val secondMap: Map[String, Int] = second.toMap
          //            key = secondMap.keySet.head
          //            result = s"${key}:(first -> iterable:(${ArrayBuffer()}),(second -> iterable:(${secondMap.values.toBuffer})"
          //          }
          //          out.collect(result)
        }
      })

    coGroupStream.print()
    env.executeAsync("A11_WindowCoGroupTest")
  }
}