package flinkTest.tableApi.timeAttribute

import java.time.Instant

import flinkTest.tableApi.InsertOnlyStream.A1_FromDataStreamTest.User
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{Schema, Table}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/**
 * @Auther: wxf
 * @Date: 2022/10/21 10:02:25
 * @Description: A2_RowtimeTest
 * @Version 1.0.0
 */
object A2_RowtimeTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 1.在 DDL 中定义
    tableEnv.executeSql(
      """
        |CREATE TABLE user_actions (
        |  user_name STRING,
        |  data STRING,
        |  user_action_time TIMESTAMP_LTZ(3),
        |  -- 声明 user_action_time 是事件时间属性，并且用 延迟 5 秒的策略来生成 watermark
        |  watermark for user_action_time as user_action_time - interval '5' second
        |) WITH (
        |'connector'='datagen',
        |'fields.user_name.length' = '5',
        |'fields.data.length' = '3'
        |)
        |""".stripMargin)
    val table: Table = tableEnv.sqlQuery("select user_name, data, user_action_time from user_actions")
    table.printSchema()
    //    (
    //      `user_name` STRING,
    //      `data` STRING,
    //      `user_action_time` TIMESTAMP_LTZ(3) *ROWTIME*
    //    )

    // 2.DataStream 到 Table 转换时定义
    // 创建数据流
    val dataStream: DataStream[User] = env.fromElements(
      User("Alice", 4, Instant.ofEpochMilli(1000)),
      User("Bob", 6, Instant.ofEpochMilli(1001)),
      User("Alice", 10, Instant.ofEpochMilli(1002)))

    val table3: Table = tableEnv.fromDataStream(dataStream,
      Schema.newBuilder()
        .columnByExpression("rowtime", "cast(event_time as TIMESTAMP_LTZ(3))")
        .watermark("rowtime", "rowtime - INTERVAL '10' SECOND")
        .build()
    )
    table3.printSchema()

    // 已经为'dataStream'定义了水印策略 直接从流中获取
    val table4: Table = tableEnv.fromDataStream(dataStream,
      Schema.newBuilder()
        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
        .watermark("rowtime", "SOURCE_WATERMARK()")
        .build()
    )
    table4.printSchema()

    val table5: Table = tableEnv.fromDataStream(
      dataStream,
      Schema.newBuilder()
        .column("name", "STRING")
        .column("score", "INT")
        .column("event_time", "TIMESTAMP_LTZ(3)")
        .watermark("event_time", "SOURCE_WATERMARK()")
        .build())
    table5.printSchema()

  }
}