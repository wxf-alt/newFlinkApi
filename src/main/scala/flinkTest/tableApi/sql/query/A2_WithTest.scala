package flinkTest.tableApi.sql.query

import bean.SensorReading
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._

/**
 * @Auther: wxf
 * @Date: 2022/10/26 16:40:24
 * @Description: A2_WithTest  With 子句测试
 * @Version 1.0.0
 */
object A2_WithTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 从外部源读取一个数据流
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val dataStream: DataStream[SensorReading] = inputStream.map(x => {
      val str: Array[String] = x.split(" ")
      SensorReading(str(0), str(1).toLong, str(2).toDouble)
    })

    tableEnv.createTemporaryView("SENSOR", dataStream, $"user", $"time", $"temp")

    val tableResult: TableResult = tableEnv.executeSql(
      """
        |with user_with_temp as(
        | SELECT user, temp + 10 AS NEW_TEMP FROM SENSOR
        |)
        |SELECT user, SUM(NEW_TEMP)
        |FROM user_with_temp
        |GROUP BY user
        |""".stripMargin)
    tableResult.print()

  }
}