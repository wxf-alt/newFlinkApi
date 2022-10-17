package flinkTest.dataStream.processFuntion

import java.time.Duration

import flinkTest.dataStream.operators.A10_IntervalJoinTest.filterFuntion
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/10/13 19:57:00
 * @Description: A2_ProcessFunctionCountWithTimestampTest
 * @Version 1.0.0
 */
object A2_ProcessFunctionCountWithTimestampTest {
  def main(args: Array[String]): Unit = {
    //生成配置对象
    val conf: Configuration = new Configuration()
    conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    // 获取 source
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)


    val mapStream: DataStream[(String, String)] = inputStream
      .map(x => {
        val str: Array[String] = x.split(" ")
        (str(0), (str(1).toLong * 1000).toString)
      })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(5000))
        .withTimestampAssigner(new SerializableTimestampAssigner[(String, String)] {
          override def extractTimestamp(element: (String, String), recordTimestamp: Long) = element._2.toLong
        })
      )
    mapStream.print("mapStream: ")

    val processStream: DataStream[(String, Long)] = mapStream
      .keyBy(_._1)
      .process(new CountWithTimeoutFunction())

    processStream.print()
    env.executeAsync("A2_ProcessFunctionCountWithTimestampTest")

  }

  case class CountWithTimestamp(key: String, count: Long, lastModified: Long)

  class CountWithTimeoutFunction extends KeyedProcessFunction[String, (String, String), (String, Long)] {

    /** The state that is maintained by this process function */
    lazy val state: ValueState[CountWithTimestamp] = getRuntimeContext.getState(new ValueStateDescriptor[CountWithTimestamp]("myState", classOf[CountWithTimestamp]))

    override def processElement(value: (String, String), ctx: KeyedProcessFunction[String, (String, String), (String, Long)]#Context, out: Collector[(String, Long)]): Unit = {
      // initialize or retrieve/update the state
      println("timestamp：" + ctx.timestamp)
      val current: CountWithTimestamp = state.value match {
        case null =>
          CountWithTimestamp(value._1, 1, ctx.timestamp)
        case CountWithTimestamp(key, count, lastModified) =>
          CountWithTimestamp(key, count + 1, ctx.timestamp)
      }

      // write the state back
      state.update(current)

      println("lastModified：" + current.lastModified)
      println("lastModified + 60000：" + (current.lastModified + 60000))
      // schedule the next timer 60 seconds from the current event time
      ctx.timerService.registerEventTimeTimer(current.lastModified + 60000)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, String), (String, Long)]#OnTimerContext, out: Collector[(String, Long)]): Unit = {
      println("onTimer(timestamp): " + timestamp)
      state.value match {
        case CountWithTimestamp(key, count, lastModified) => {
          println(s"进入 ${lastModified} 定时器")
          out.collect((key, count))
//          if (timestamp == lastModified + 60000) {
//          }
        }
        case _ =>
      }
    }

  }

}
