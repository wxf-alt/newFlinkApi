package flinkTest.tableApi.sql.query

import bean.SensorReading
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row

/**
 * @Auther: wxf
 * @Date: 2022/10/26 14:44:48
 * @Description: A1_SimpleSelectTest  对已注册表和内联表指定 SQL 查询。
 * @Version 1.0.0
 */
object A1_SimpleSelectTest {
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

    // 带有内联(未注册)表的SQL查询
    val table: Table = dataStream.toTable(tableEnv, $"user", $"product", $"amount")
    val result: Table = tableEnv.sqlQuery(s"SELECT SUM(amount) FROM $table WHERE user LIKE '%1%'")
    result.toRetractStream[Row].print("result1")


    // 带有注册表的SQL查询
    //  将DataStream注册为“orders”
    tableEnv.createTemporaryView("orders", dataStream, $"user", $"product", $"amount")
    // 在表上运行SQL查询，并将结果作为一个新表检索
    val result2: Table = tableEnv.sqlQuery("SELECT SUM(amount) FROM orders WHERE user LIKE '%1%'")
    result2.toRetractStream[Row].print("result2")

    env.execute("A1_SimpleSelectTest")
  }
}