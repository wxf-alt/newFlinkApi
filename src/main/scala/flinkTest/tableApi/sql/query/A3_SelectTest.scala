package flinkTest.tableApi.sql.query

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._

/**
 * @Auther: wxf
 * @Date: 2022/10/26 16:56:56
 * @Description: A3_SelectTest
 * @Version 1.0.0
 */
object A3_SelectTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 使用 VALUES 子句 生成测试数据
    val table: Table = tableEnv.sqlQuery(
      """
        |SELECT order_id, price FROM (VALUES(1,2.0),(2,3.1)) AS t(order_id,price)
        |""".stripMargin)

    table.printSchema()
    table.execute().print()

  }
}