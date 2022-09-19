package flinkTest.dataStream

import java.time.Duration

import bean.Sensor
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/9/13 16:15:58
 * @Description: a3_waterMarkTest   设置 WaterMark
 * @Version 1.0.0
 */
object a3_WaterMarkTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8888)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    // 生成 waterMark 间隔时间
    env.getConfig.setAutoWatermarkInterval(1000)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)

    // 设置 有界无序 waterMark
    val waterMarkStartegies: WatermarkStrategy[Sensor] = WatermarkStrategy
      .forBoundedOutOfOrderness[Sensor](Duration.ofMillis(2000)) // waterMark 延迟时间
      //      .withWatermarkAlignment("alignment-group-1", Duration.ofSeconds(20), Duration.ofSeconds(1))   // 设置 waterMark 对齐
      .withIdleness(Duration.ofMinutes(5)) // 设置 空闲数据源超时,不会阻碍下游运算符中的水印进程
      .withTimestampAssigner(new SerializableTimestampAssigner[Sensor] {
        override def extractTimestamp(element: Sensor, recordTimestamp: Long): Long = element.timeStamp * 1000
      })

    val mapStream: DataStream[Sensor] = inputStream.map(x => {
      val str: Array[String] = x.split(" ")
      Sensor(str(0), str(1).toLong, str(2).toDouble)
    }).assignTimestampsAndWatermarks(waterMarkStartegies)

    mapStream.print("mapStream：")

    env.execute("a3_waterMarkTest")
  }
}
