package flinkTest.dataStream.window

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * @Auther: wxf
 * @Date: 2022/10/11 19:47:58
 * @Description: A7_CustomTriggerTest  自定义触发器
 * @Version 1.0.0
 */
object A7_CustomTriggerTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val keyedStream: KeyedStream[(String, Int), String] = inputStream.map((_, 1)).keyBy(_._1)

    val windowStream: DataStream[(String, Int)] = keyedStream
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .trigger(CustomTrigger())
      .reduce((x, y) => (x._1, x._2 + y._2))

    windowStream.print()
    env.executeAsync("A7_CustomTriggerTest")
  }

  case class CustomTrigger() extends Trigger[(String, Int), TimeWindow] {

    // 输入一个元素  什么都不做
    // Trigger 还可以在 window 被创建后、删除前的这段时间内定义 清理（purge）窗口中的数据
    override def onElement(element: (String, Int), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      if (element._1 == "aa") {
        TriggerResult.PURGE
      } else {
        TriggerResult.CONTINUE
      }
      //      TriggerResult.CONTINUE
    }

    // 方法在注册的 processing-time timer 触发时调用
    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE

    // 方法在注册的 event-time timer 触发时调用
    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    // clear() 方法处理在对应窗口被移除时所需的逻辑
    // 删除给定时间的处理时间触发器
    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = ctx.deleteProcessingTimeTimer(window.maxTimestamp)

  }

}
