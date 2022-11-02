package flinkTest.tableApi.operators

import java.time.Duration

import bean.SensorReading
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row

/**
 * @Auther: wxf
 * @Date: 2022/10/24 19:28:04
 * @Description: A6_OrderbyTest
 * @Version 1.0.0
 */
object A6_OrderbyTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 环境配置
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val dataStream: DataStream[SensorReading] = inputStream.map(x => {
      val str: Array[String] = x.split(" ")
      SensorReading(str(0), str(1).toLong, str(2).toDouble)
    }) // 设置 waterMark
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading) = element.timestamp * 1000L
      })

    // 方式一：DataStream 转换为 Table 指定时间语义
    //    // 设置 EventTime   指定字段 或者 新增字段 指定rowtime
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'timestamp.rowtime as 'ts, 'temperature)
    sensorTable.printSchema()

    sensorTable.orderBy('ts.desc, 'temperature).offset(10).fetch(5).toRetractStream[Row].filter(_._1).print()

    env.execute("A6_OrderbyTest")
  }
}