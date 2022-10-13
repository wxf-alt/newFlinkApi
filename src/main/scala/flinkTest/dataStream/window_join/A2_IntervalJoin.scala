package flinkTest.dataStream.window_join

import java.text.SimpleDateFormat
import java.time.Duration
import java.util.Date

import flinkTest.dataStream.operators.A10_IntervalJoinTest.filterFuntion
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/10/12 17:35:41
 * @Description: A2_IntervalJoin
 * @Version 1.0.0
 */
object A2_IntervalJoin {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val inputStream1: DataStream[String] = env.socketTextStream("localhost", 6666)
    val inputStream2: DataStream[String] = env.socketTextStream("localhost", 7777)

    val keyedStream1: KeyedStream[(String, Long, Int), String] = inputStream1
      .filter(filterFuntion(_))
      .map(x => {
        val str: Array[String] = x.split(" ")
        (str(0), str(1).toLong * 1000, str(2).toInt)
      })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(5000))
        .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long, Int)] {
          override def extractTimestamp(element: (String, Long, Int), recordTimestamp: Long) = element._2
        })
      )
      .keyBy(_._1)

    val keyedStream2: KeyedStream[(String, Long, Int), String] = inputStream2
      .filter(filterFuntion(_))
      .map(x => {
        val str: Array[String] = x.split(" ")
        (str(0), str(1).toLong * 1000, str(2).toInt)
      })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(5000))
        .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long, Int)] {
          override def extractTimestamp(element: (String, Long, Int), recordTimestamp: Long) = element._2
        })
      )
      .keyBy(_._1)

    keyedStream2.print()
    keyedStream1.print()

    // key1 == key2 && leftTs + lowerBound  < rightTs < leftTs + upperBound
    // key值相等；并且 右边的数据时间戳 要在 左边数据时间戳 +- 指定的上下界之间
    val intervalJoinStream: DataStream[(String, String, String, String, Int, Int)] = keyedStream1.intervalJoin(keyedStream2)
      .between(Time.seconds(-5), Time.seconds(5))
      .lowerBoundExclusive() // 排除 下界 时间戳
      .upperBoundExclusive() // 排除 上界 时间戳
      .process(MyProcessJoinFunction())

    intervalJoinStream.print()
    env.executeAsync("A2_IntervalJoin")

  }

  case class MyProcessJoinFunction() extends ProcessJoinFunction[(String, Long, Int), (String, Long, Int), (String, String, String, String, Int, Int)] {

    var dateFormat: SimpleDateFormat = _

    override def open(parameters: Configuration) = {
      dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")
    }

    override def processElement(left: (String, Long, Int), right: (String, Long, Int),
                                ctx: ProcessJoinFunction[(String, Long, Int), (String, Long, Int), (String, String, String, String, Int, Int)]#Context,
                                out: Collector[(String, String, String, String, Int, Int)]) = {
      // timestamp 会从两个元素的 timestamp 中取最大值
      val timestamp: Long = ctx.getTimestamp
      val leftTimestamp: Long = ctx.getLeftTimestamp
      val rightTimestamp: Long = ctx.getRightTimestamp

      val timeStampDate: String = dateFormat.format(new Date(timestamp))
      val leftTimestampDate: String = dateFormat.format(new Date(leftTimestamp))
      val rightTimestampDate: String = dateFormat.format(new Date(rightTimestamp))
      val key: String = left._1
      out.collect((key, timeStampDate, leftTimestampDate, rightTimestampDate, left._3, right._3))
    }
  }

}
