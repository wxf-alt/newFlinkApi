package flinkTest.connect.tale

import bean.Sensor
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{Table}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._

/**
 * @Auther: wxf
 * @Date: 2022/11/9 10:29:35
 * @Description: JDBCConnectionTest
 * @Version 1.0.0
 */
object JDBCConnectionTest {
  def main(args: Array[String]): Unit = {
    //    val conf: Configuration = new Configuration()
    //    conf.setString(RestOptions.BIND_PORT, "8081-8089");
    //    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    //            val settings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().build()
    //            val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val mapStream: DataStream[Sensor] = inputStream.map(x => {
      val str: Array[String] = x.split(" ")
      Sensor(str(0), str(1) toLong, str(2).toDouble)
    })

    val inputTable: Table = tableEnv.fromDataStream(mapStream)
    val mapTable: Table = inputTable.renameColumns($"id" as "sensor", $"timeStamp" as "ts")
    tableEnv.createTemporaryView("mapTable", mapTable)

    val createSql: String =
      """
        |create table MyUserTable(
        | sensor string,
        | temperature double,
        | ts bigint,
        | PRIMARY KEY (sensor) NOT ENFORCED
        |) WITH (
        |   'connector' = 'jdbc',
        |   'url' = 'jdbc:mysql://localhost:3306/db',
        |   'username'='root',
        |   'password'='root',
        |   'table-name' = 'sensor'
        |)
        |""".stripMargin
    tableEnv.executeSql(createSql)

    tableEnv.executeSql(
      s"""
         |insert into MyUserTable select sensor, temperature, ts from mapTable
         |""".stripMargin)


    //    // 读取 JDBC 数据
    //    //    不会重复读取 只会执行一次
    //    val createSql: String =
    //      """
    //        |create table MyUserTable(
    //        | sensor string,
    //        | temperature double,
    //        | ts bigint,
    //        | PRIMARY KEY (sensor) NOT ENFORCED
    //        |) WITH (
    //        |   'connector' = 'jdbc',
    //        |   'url' = 'jdbc:mysql://localhost:3306/db',
    //        |   'username'='root',
    //        |   'password'='root',
    //        |   'table-name' = 'sensor'
    //        |)
    //        |""".stripMargin
    //    tableEnv.executeSql(createSql)
    //    tableEnv.sqlQuery("select * from MyUserTable").execute().print()


  }
}