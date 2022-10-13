package flinkTest.dataStream.operators

import java.text.SimpleDateFormat
import java.time.Duration
import java.util.Date

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/10/9 17:26:49
 * @Description: A10_IntervalJoinTest   Interval join 目前仅支持 event time   需要设置 指定时间字段
 *               Interval Join 基于 KeyedStream 进行连接
 * @Version 1.0.0
 */
object A10_IntervalJoinTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    env.getConfig.setAutoWatermarkInterval(2000)

    val inputStream1: DataStream[String] = env.socketTextStream("localhost", 6666)
    val inputStream2: DataStream[String] = env.socketTextStream("localhost", 7777)

    val keyedStream1: KeyedStream[(String, Long, Int), String] = inputStream1
      .filter(filterFuntion(_))
      .map(x => {
        val str: Array[String] = x.split(" ")
        (str(0), str(1).toLong, str(2).toInt)
      })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(500))
        .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long, Int)] {
          override def extractTimestamp(element: (String, Long, Int), recordTimestamp: Long) = element._2
        })
      )
      .keyBy(_._1)

    val keyedStream2: KeyedStream[(String, Long, Int), String] = inputStream2
      .filter(filterFuntion(_))
      .map(x => {
        val str: Array[String] = x.split(" ")
        (str(0), str(1).toLong, str(2).toInt)
      })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(500))
        .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long, Int)] {
          override def extractTimestamp(element: (String, Long, Int), recordTimestamp: Long) = element._2
        })
      )
      .keyBy(_._1)

    keyedStream1.print()
    keyedStream2.print()

    // key1 == key2 && leftTs - 2 < rightTs < leftTs + 2
    // key值相等；并且 右边的数据时间戳 要在 左边数据时间戳 +- 指定的上下界之间
    val intervalJoinStream: DataStream[(String, String, String, String, Int)] = keyedStream1.intervalJoin(keyedStream2)
      .between(Time.seconds(-2), Time.seconds(2))
      .process(new ProcessJoinFunction[(String, Long, Int), (String, Long, Int), (String, String, String, String, Int)] {
        override def processElement(left: (String, Long, Int), right: (String, Long, Int),
                                    ctx: ProcessJoinFunction[(String, Long, Int), (String, Long, Int), (String, String, String, String, Int)]#Context,
                                    out: Collector[(String, String, String, String, Int)]) = {
          val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")
          // 获取时间戳
          val leftTime: Date = new Date(ctx.getLeftTimestamp)
          val rightTime: Date = new Date(ctx.getRightTimestamp)
          val currentTime: Date = new Date(ctx.getTimestamp)
          // 拼接结果
          val resultSum: Int = left._3 + right._3
          out.collect((left._1, dateFormat.format(leftTime), dateFormat.format(rightTime), dateFormat.format(currentTime), resultSum))
        }
      })

    intervalJoinStream.print()
    env.executeAsync("A10_IntervalJoinTest")

  }


  val filterFuntion: String => Boolean = (x: String) => {
    val str: Array[String] = x.split(" ")
    var num: Any = str(2)
    try {
      num = str(2).toInt
    } catch {
      case exception: Exception => ""
    }
    str.length == 3 && num.isInstanceOf[Int]
  }


}
