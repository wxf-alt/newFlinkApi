package flinkTest.dataStream.processFuntion

import bean.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Description: A1_ProcessFunctionSensorTest 测试
 * @Author: wxf
 * @Date: 2022/10/13 19:51
 **/
// 需求：监控温度传感器的温度值，如果温度值在 10秒钟之内(processing time)连续上升，则报警。
object A1_ProcessFunctionSensorTest {
  def main(args: Array[String]): Unit = {
    //生成配置对象
    val conf: Configuration = new Configuration()
    conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    environment.setParallelism(1)

    // 获取 source
    val inputStream: DataStream[String] = environment.socketTextStream("localhost", 6666)

    // 解析数据
    val warningStream: DataStream[String] = inputStream.map(x => {
      val str: Array[String] = x.split(" ")
      SensorReading(str(0), str(1).toLong, str(2).toDouble)
    }).keyBy("id")
      .process(new MyKeyedProcessFuntion(5000L))

    warningStream.print()

    environment.execute("A1_ProcessFunctionSensorTest")
  }
}

class MyKeyedProcessFuntion(interval: Long) extends KeyedProcessFunction[Tuple, SensorReading, String] {

  // 由于需要跟之前的温度值做对比，所以将上一个温度保存成状态
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  //  private lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", Types.of[Double]))
  // 为了方便删除定时器，还需要保存定时器的时间戳
  lazy val curTimerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer", classOf[Long]))
  //  private lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer", Types.of[Long]))

  // 主要逻辑
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // 获取状态中 存储的值
    val lastTemp: Double = lastTempState.value()
    val curTimerTs: Long = curTimerTsState.value()
    // 将上次温度值的状态更新为当前数据的温度值
    lastTempState.update(value.temperature)

    // 进行判断 是否连续上升;并且没有定时器的话，注册10秒后的定时器
    if (value.temperature > lastTemp && curTimerTs == 0) {
      // 获取当前时间  用于定时器使用
      val ts: Long = ctx.timerService().currentProcessingTime() + interval

      //      // 如果是 事件时间语义 的话 可以使用 WaterMark 作为 定时器时间戳
      //      val ts: Long = ctx.timerService().currentWatermark() + interval

      // 注册定时器  并 更新 定时器的时间戳 状态
      println(s"${ctx.getCurrentKey.asInstanceOf[Tuple1[String]].f0} 注册定时器：${ts}")
      ctx.timerService().registerProcessingTimeTimer(ts)
      curTimerTsState.update(ts)
    } else if (value.temperature < lastTemp) { // 温度下降
      // 删除定时器
      println(s"${ctx.getCurrentKey.asInstanceOf[Tuple1[String]].f0} 删除定时器")
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      // 清空状态
      curTimerTsState.clear()
    }
  }

  // 定时器触发，说明10秒内没有来下降的温度值，报警
  // timestamp 定时器所触发的时间
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    val key = ctx.getCurrentKey.asInstanceOf[Tuple1[String]].f0
    out.collect(s"${key} 的温度值连续在 ${timestamp} 时间" + interval / 1000 + "秒上升")
    curTimerTsState.clear()
  }

}