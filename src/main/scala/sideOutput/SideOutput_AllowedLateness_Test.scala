package sideOutput

import java.time.Duration

import bean.Sensor
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow}
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/9/1 14:41:56
 * @Description: SideOutput_AllowedLateness_Test
 * @Version 1.0.0
 */
object SideOutput_AllowedLateness_Test {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 设置 waterMark 生成周期
    env.getConfig.setAutoWatermarkInterval(300L)

    // 设置 waterMark  2秒延迟
    val strategy: WatermarkStrategy[Sensor] = WatermarkStrategy.forBoundedOutOfOrderness[Sensor](Duration.ofSeconds(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[Sensor]() {
        override def extractTimestamp(element: Sensor, recordTimestamp: Long) = element.timeStamp
      })

    // 测输出流
    val lateOutputTag: OutputTag[Sensor] = new OutputTag[Sensor]("late")

    val socketStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val mapStream: DataStream[Sensor] = socketStream.map(x => {
      val str: Array[String] = x.split(" ")
      Sensor(str(0), str(1).toLong * 1000, str(2).toDouble)
    }).assignTimestampsAndWatermarks(strategy)
    mapStream.print("mapStream：")

    val result: DataStream[(String, Long, Long, Double)] = mapStream.keyBy(x => x.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .allowedLateness(Time.seconds(1)) // 允许 2 秒 迟到
      .sideOutputLateData(lateOutputTag)
      .process(new MyProcessWindowFunction())

    // 获取 迟到数据(窗口结束时间 + allowedLateness延迟时间 之后到达的数据)
    val lateStream: DataStream[Sensor] = result.getSideOutput(lateOutputTag)

    result.print("result：")
    lateStream.print("lateStream：")

    env.execute("SideOutput_AllowedLateness_Test")
  }
}

class MyProcessWindowFunction extends ProcessWindowFunction[Sensor, (String, Long, Long, Double), String, TimeWindow] {

  override def process(key: String, context: Context, elements: Iterable[Sensor], out: Collector[(String, Long, Long, Double)]): Unit = {
    var max: Double = 0D
    for (elem <- elements) {
      max = Math.max(elem.temperature, max)
    }
    out.collect((key, context.window.getStart, context.window.getEnd, max))
  }
}
