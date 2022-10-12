package flinkTest.dataStream.operators

import java.lang

import flinkTest.dataStream.t1_operators.A5_ReduceTest.filterFuntion
import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

/**
 * @Auther: wxf
 * @Date: 2022/10/10 14:34:07
 * @Description: A11_WindowCoGroupTest
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
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .apply(new CoGroupFunction[(String, Int), (String, Int), String] {
        override def coGroup(first: lang.Iterable[(String, Int)], second: lang.Iterable[(String, Int)], out: Collector[String]) = {

          import scala.collection.convert.ImplicitConversions._

          //          val leftList: List[Int] = List[Int]()
          //          val rightList: List[Int] = List[Int]()
          var key: String = null
          var result: String = ""

          if (first.size != 0 && second.size != 0) {
            val firstMap: Map[String, Int] = first.toMap
            val secondMap: Map[String, Int] = second.toMap
            key = firstMap.keySet.head
            result = s"${key}:(first -> iterable:(${firstMap.values.toBuffer}),(second -> iterable:(${secondMap.values.toBuffer})"
          } else if (first.size != 0) {
            val firstMap: Map[String, Int] = first.toMap
            key = firstMap.keySet.head
            result = s"${key}:(first -> iterable:(${firstMap.values.toBuffer}),(second -> iterable:(${ArrayBuffer()})"
          } else if (second.size != 0) {
            val secondMap: Map[String, Int] = second.toMap
            key = secondMap.keySet.head
            result = s"${key}:(first -> iterable:(${ArrayBuffer()}),(second -> iterable:(${secondMap.values.toBuffer})"
          }
          out.collect(result)
        }
      })

    coGroupStream.print()
    env.executeAsync("A11_WindowCoGroupTest")
  }
}