package flinkTest.dataStream


import java.text.SimpleDateFormat
import java.time.{Duration, LocalDateTime}
import java.util.Date

import bean.Sensor
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, Watermark, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, RichProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/9/13 16:15:58
 * @Description: a4_waterMarkTest   自定义 WaterMark
 * @Version 1.0.0
 */
object a4_WaterMarkCustomkTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    // 生成 waterMark 间隔时间
    env.getConfig.setAutoWatermarkInterval(1000)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val mapStream: DataStream[Sensor] = inputStream.map(x => {
      val str: Array[String] = x.split(" ")
      Sensor(str(0), str(1).toLong * 1000, str(2).toDouble)
    })
      //      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Sensor](Duration.ofMillis(3500L))
      //        .withTimestampAssigner(new SerializableTimestampAssigner[Sensor] {
      //          override def extractTimestamp(element: Sensor, recordTimestamp: Long): Long = element.timeStamp * 1000
      //        }))
      //  测试 自定义 waterMark
      .assignTimestampsAndWatermarks(new WatermarkStrategy[Sensor] {
        override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[Sensor] = {
          new PunctuatedAssigner()
        }
      }.withTimestampAssigner(new SerializableTimestampAssigner[Sensor] {
        override def extractTimestamp(element: Sensor, recordTimestamp: Long): Long = element.timeStamp
      })
        .withIdleness(Duration.ofSeconds(2)) // 设置空闲时间超时 2秒
      )

    val windowStream: DataStream[(String, String, String, String, Double)] = mapStream.keyBy(_.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .process(new MyAvgTempProcessWindowFunction())

    windowStream.print("mapStream：")

    env.execute("a4_waterMarkCustomkTest")
  }
}

class MyAvgTempProcessWindowFunction extends ProcessWindowFunction[Sensor, (String, String, String, String, Double), String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[Sensor], out: Collector[(String, String, String, String, Double)]): Unit = {
    val size: Int = elements.size
    val sum: Double = elements.map(x => {
      x.temperature
    }).reduce(_ + _)
    val avgTemp: Double = sum / size

    val start: Long = context.window.getStart
    val end: Long = context.window.getEnd
    val watermark: Long = context.currentWatermark

    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")
    val start_date: Date = new Date(start)
    val end_date: Date = new Date(end)
    val watermark_date: Date = new Date(watermark)

    out.collect((key, dateFormat.format(start_date), dateFormat.format(end_date), dateFormat.format(watermark_date), avgTemp))
  }
}

/**
 * @Description:
 * @Author: wxf
 * @Date: 2022/9/13 19:54   周期性 Watermark
 *        该 watermark 生成器可以覆盖的场景是：数据源在一定程度上乱序。
 *        即某个最新到达的时间戳为 t 的元素将在最早到达的时间戳为 t 的元素之后最多 n 毫秒到达。
 **/
class BoundedOutOfOrdernessGenerator1 extends WatermarkGenerator[Sensor] {
  val maxOutOfOrderness: Long = 3500L // 3.5 秒

  var currentMaxTimestamp: Long = _

  override def onEvent(event: Sensor, eventTimestamp: Long, output: WatermarkOutput): Unit = {
    currentMaxTimestamp = Math.max(eventTimestamp, currentMaxTimestamp)
  }

  override def onPeriodicEmit(output: WatermarkOutput): Unit = {
    // 发出的 watermark = 当前最大时间戳 - 最大乱序时间
    output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1))
  }
}

/**
 * @Description:
 * @Author: wxf
 * @Date: 2022/9/13 20:45    周期性 Watermark
 *        该生成器生成的 watermark 滞后于 处理时间 固定量。它假定元素会在有限延迟后到达 Flink
 **/
class TimeLagWatermarkGenerator extends WatermarkGenerator[Sensor] {

  val maxTimeLag: Long = 2000L // 3.5 秒

  override def onEvent(event: Sensor, eventTimestamp: Long, output: WatermarkOutput): Unit = {}

  override def onPeriodicEmit(output: WatermarkOutput): Unit = {
    output.emitWatermark(new Watermark(System.currentTimeMillis() - maxTimeLag))
  }
}


/**
 * @Description:
 * @Author: wxf
 * @Date: 2022/9/13 20:48   标记性 Watermark
 *        标记生成器的方法，当事件带有某个指定标记时，该生成器就会发出 watermark：2秒延迟
 **/
class PunctuatedAssigner extends WatermarkGenerator[Sensor] {

  val maxOutOfOrderness: Long = 2000L // 2 秒

  var currentMaxTimestamp: Long = 0L

  override def onEvent(event: Sensor, eventTimestamp: Long, output: WatermarkOutput): Unit = {
    if (event.id == "sensor_1") {
      currentMaxTimestamp = Math.max(currentMaxTimestamp, event.timeStamp)
      output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness))
    }
  }

  override def onPeriodicEmit(output: WatermarkOutput): Unit = {}
}