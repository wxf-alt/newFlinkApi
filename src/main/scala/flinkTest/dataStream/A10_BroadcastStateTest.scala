package flinkTest.dataStream

import bean.Sensor
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/9/15 17:37:26
 * @Description: a10_broadcastStateTest   测试 广播状态
 *              主流输入 传感器数据，广播流传入(传感器id,温度值)   输出当前传感器id的温度 小于 广播流设定的规则的温度值
 * @Version 1.0.0
 */
object A10_BroadcastStateTest extends App {

  val conf: Configuration = new Configuration()
  conf.setInteger(RestOptions.PORT, 8081)
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
  env.setParallelism(1)

  val socketSource1: DataStream[String] = env.socketTextStream("localhost", 6666)
  val mapStream1: DataStream[Sensor] = socketSource1.map(x => {
    val str: Array[String] = x.split(" ")
    Sensor(str(0), str(1).toLong * 1000, str(2).toDouble)
  })
  // 获取 传感器 流
  val sensorPartitionedStream: KeyedStream[Sensor, String] = mapStream1.keyBy(new KeySelector[Sensor, String] {
    override def getKey(value: Sensor): String = value.id
  })

  // 获取 规则 流
  val socketSource2: DataStream[String] = env.socketTextStream("localhost", 7777)
  val mapStream2: DataStream[(String, Double)] = socketSource2.map(x => {
    val str: Array[String] = x.split(" ")
    (str(0), str(1).toDouble)
  })
  // 定义 Map 状态
  val ruleStateDescriptor: MapStateDescriptor[String, (String, Double)] = new MapStateDescriptor("RulesBroadcastState",
    TypeInformation.of(classOf[String]),
    TypeInformation.of(classOf[(String, Double)]))
  val ruleBroadcastStream: BroadcastStream[(String, Double)] = mapStream2.broadcast(ruleStateDescriptor)

  // 连接 两个流
  val connectStream: DataStream[Sensor] = sensorPartitionedStream.connect(ruleBroadcastStream)
    .process(new KeyedBroadcastProcessFunction[String, Sensor, (String, Double), Sensor] {

      // 定义主流状态
      val sensorMapStateDesc: MapStateDescriptor[String, Sensor] = new MapStateDescriptor("sensor",
        TypeInformation.of(classOf[String]),
        TypeInformation.of(classOf[Sensor]))
      // 定义广播流状态
      val ruleStateDescriptor: MapStateDescriptor[String, (String, Double)] = new MapStateDescriptor("RulesBroadcastState",
        TypeInformation.of(classOf[String]),
        TypeInformation.of(classOf[(String, Double)]))

      override def processBroadcastElement(value: (String, Double), ctx: KeyedBroadcastProcessFunction[String, Sensor, (String, Double), Sensor]#Context, out: Collector[Sensor]) = {
        // 获取广播流 并更新广播流数据
        ctx.getBroadcastState(ruleStateDescriptor).put(value._1, value)
      }

      override def processElement(value: Sensor, ctx: KeyedBroadcastProcessFunction[String, Sensor, (String, Double), Sensor]#ReadOnlyContext, out: Collector[Sensor]) = {
        // 获取主流数据
        val state: MapState[String, Sensor] = getRuntimeContext.getMapState(sensorMapStateDesc)

        // 获取广播流数据
        import scala.collection.convert.ImplicitConversions._
        for (elem <- ctx.getBroadcastState(ruleStateDescriptor).immutableEntries()) {
          val ruleName: String = elem.getKey
          val ruleBroadcast: (String, Double) = elem.getValue

          var sensorState: Sensor = state.get(ruleName)
          // 更新状态
          state.put(value.id, value)

          val sensor: Sensor = if (null == sensorState) {
            Sensor("sen", 0L, 0.0D)
          } else {
            sensorState
          }

          if (ruleName == sensor.id && ruleBroadcast._2 > sensor.temperature) {
            out.collect(sensor)
          }

        }
      }

    })

  connectStream.print("connectStream：")

  env.execute("a10_broadcastStateTest")
}
