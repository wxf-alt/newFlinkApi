package flinkTest

import java.time.Duration

import bean.Sensor
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.{CheckpointingMode, TimerService}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/9/1 20:35:29
 * @Description: ProcessFuntionSimulationWindow  使用 Process Function 模拟 window。计算 每5秒内每个传感器的温度总和。并且处理迟到数据 侧输出
 * @Version 1.0.0
 */
object ProcessFuntionSimulationWindow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 设置 waterMark 生成周期
    env.getConfig.setAutoWatermarkInterval(500L)

    // 设置 waterMark  1秒延迟
    val strategy: WatermarkStrategy[Sensor] = WatermarkStrategy.forBoundedOutOfOrderness[Sensor](Duration.ofSeconds(1))
      .withTimestampAssigner(new SerializableTimestampAssigner[Sensor]() {
        override def extractTimestamp(element: Sensor, recordTimestamp: Long): Long = element.timeStamp
      })

    val socketStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val mapStream: DataStream[Sensor] = socketStream.map(x => {
      val str: Array[String] = x.split(" ")
      Sensor(str(0), str(1).toLong * 1000, str(2).toDouble)
    }).assignTimestampsAndWatermarks(strategy)

    mapStream.print("mapStream：")

    val result: DataStream[(String, Long, Long, Double)] = mapStream.keyBy(x => x.id)
      .process(new PseudoWindow(Time.seconds(5)))

    result.print("result：")
    result.getSideOutput(new OutputTag[Sensor]("lateFares")).print("lateFares：")

    env.execute("ProcessFuntionSimulationWindow")

  }
}

class PseudoWindow(duration: Time) extends KeyedProcessFunction[String, Sensor, (String, Long, Long, Double)] {

  // 定义状态
  var sumOfTips: MapState[(Long, Long), Double] = _
  // 设置 时间长度
  var milliseconds: Long = _

  override def open(parameters: Configuration): Unit = {
    val sumDesc: MapStateDescriptor[(Long, Long), Double] = new MapStateDescriptor[(Long, Long), Double]("sumOfTips", classOf[(Long, Long)], classOf[Double])
    sumOfTips = getRuntimeContext.getMapState(sumDesc)
    milliseconds = duration.toMilliseconds
  }

  override def processElement(value: Sensor, ctx: KeyedProcessFunction[String, Sensor, (String, Long, Long, Double)]#Context, out: Collector[(String, Long, Long, Double)]): Unit = {
    // 获取 当前事件的时间 EventTime
    val eventTime: Long = value.timeStamp
    // 获取时间管理 从而获取当前的 WaterMark
    val timerService: TimerService = ctx.timerService()
    val currentEventWaterMark: Long = timerService.currentWatermark()

    println(s"当前事件 时间：${eventTime}")
    println(s"当前事件 WaterMark：${currentEventWaterMark}")

    // 判断当前事件 是否属于延迟事件
    if (eventTime <= currentEventWaterMark) {
      // 当前事件时间 <= WaterMark, 所以 是迟到数据  直接输出
      val lateOutput: OutputTag[Sensor] = new OutputTag[Sensor]("lateFares")
      ctx.output(lateOutput, value)
    } else { // 当前事件 不属于 当前 WaterMark,属于待处理数据
      // 将 eventTime 向上取值并将结果赋值到包含当前事件的窗口的起始时间点 和 末尾时间点。
      val windowStartTime: Long = eventTime - (eventTime % milliseconds)
      val windowEndTime: Long = windowStartTime + milliseconds - 1
      //      val windowEndTime: Long = windowStartTime + milliseconds
      println(s"窗口 开始时间：${windowStartTime}")
      println(s"窗口 结束时间：${windowEndTime}")

      // 调用 定时器
      timerService.registerEventTimeTimer(windowEndTime)

      // 将当前传感器的温度值添加到该窗口的总计中。
      // 从状态中 获取 当前传感器的当前时间的温度值
      var sum: Double = sumOfTips.get((windowStartTime, windowEndTime))
      if (sum == null) {
        sum = 0.0D
      }
      sum += value.temperature
      // 更新状态
      sumOfTips.put((windowStartTime, windowEndTime), sum)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Sensor, (String, Long, Long, Double)]#OnTimerContext, out: Collector[(String, Long, Long, Double)]): Unit = {
    val key: String = ctx.getCurrentKey
    // 获取 状态的值
    val startTimeSatmp: Long = timestamp + 1 - milliseconds
    //    val startTimeSatmp: Long = timestamp - milliseconds
    val sumNum: Double = sumOfTips.get((startTimeSatmp, timestamp))

    // 输出
    out.collect((key, startTimeSatmp, timestamp, sumNum))
    // 清空状态
    sumOfTips.remove((startTimeSatmp, timestamp))

  }
}
