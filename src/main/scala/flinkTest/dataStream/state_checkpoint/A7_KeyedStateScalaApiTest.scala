package flinkTest.dataStream.state_checkpoint

import bean.Sensor
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/9/14 17:38:42
 * @Description: a7_keyedStateScalaApiTest  DataStream 状态相关的 Scala API
 *              每三次计算一次平均温度
 * @Version 1.0.0
 */
object A7_KeyedStateScalaApiTest extends App {
  val conf: Configuration = new Configuration()
  conf.setInteger(RestOptions.PORT, 8081)
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
  env.setParallelism(1)

  val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)

  val mapStream: DataStream[Sensor] = inputStream.map(x => {
    val str: Array[String] = x.split(" ")
    Sensor(str(0), str(1).toLong * 1000, str(2).toDouble)
  })

  // 建议 使用 KeySelector 的方式,指定key
  val flatMapStream: DataStream[(String, Long, Double)] = mapStream
    .keyBy(_.id)
    .mapWithState((x: Sensor, y: Option[(Long, Double)]) => {
      (x, y) match {
        case (x: Sensor, None) => ((x.id, 1L, x.temperature), Some(1L, x.temperature))
        case (x: Sensor, Some(c)) => {
          if (c._1 + 1 >= 3) {
            ((x.id, c._1 + 1, (c._2 + x.temperature) / (c._1 + 1)), None)
          } else {
            ((x.id, c._1 + 1, c._2 + x.temperature), Some(c._1 + 1L, c._2 + x.temperature))
          }
        }
      }
    })

  flatMapStream.print("flatMapStream：")

  env.executeAsync("a7_keyedStateScalaApiTest")

}
