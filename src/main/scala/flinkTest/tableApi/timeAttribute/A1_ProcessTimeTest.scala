package flinkTest.tableApi.timeAttribute

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{Schema, _}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/**
 * @Auther: wxf
 * @Date: 2022/10/20 19:57:00
 * @Description: A1_ProcessTimeTest
 * @Version 1.0.0
 */
object A1_ProcessTimeTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 1.在创建表的 DDL 中定义 处理时间 AS PROCTIME()
    tableEnv.executeSql(
      """
        |CREATE TABLE user_actions (
        |  user_name STRING,
        |  data STRING,
        |  user_action_time AS PROCTIME()
        |) WITH (
        |'connector'='datagen',
        |'fields.user_name.length' = '5',
        |'fields.data.length' = '3'
        |)
        |""".stripMargin)
    val table1: Table = tableEnv.sqlQuery(
      """
        |SELECT TUMBLE_START(user_action_time, INTERVAL '10' SECOND), COUNT(DISTINCT user_name)
        |FROM user_actions
        |GROUP BY TUMBLE(user_action_time, INTERVAL '10' SECOND)
        |""".stripMargin)
    val value1: DataStream[(Boolean, Row)] = table1.toRetractStream[Row]
    value1.print()
    //    (true,+I[2022-10-20T20:21:40, 77012])
    //    (true,+I[2022-10-20T20:21:50, 95457])
    //    (true,+I[2022-10-20T20:22, 95450])

    // 2.在 DataStream 到 Table 转换时定义  处理时间
    val inputStream: DataStream[(String, String)] = env.socketTextStream("localhost", 6666)
      .map(x => {
        val str: Array[String] = x.split(" ")
        (str(0), str(1))
      })
    val table2: Table = tableEnv.fromDataStream(inputStream, $"user_name", $"data", $"user_action_time".proctime())
    table2.toAppendStream[Row].print()

    val table3: Table = tableEnv.fromDataStream(inputStream,
      // 设置 处理时间
      Schema.newBuilder().columnByExpression("proctime", "PROCTIME()").build()
    )
    table3.printSchema()

    env.execute()
  }
}