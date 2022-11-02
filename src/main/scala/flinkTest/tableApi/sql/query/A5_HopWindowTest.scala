package flinkTest.tableApi.sql.query

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/**
 * @Auther: wxf
 * @Date: 2022/10/27 17:23:21
 * @Description: A5_HopWindowTest
 * @Version 1.0.0
 */
object A5_HopWindowTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val inputStream1: DataStream[(String, Long, Double)] = env.socketTextStream("localhost", 6666)
      .map(x => {
        val str: Array[String] = x.split(" ")
        (str(0), str(1).toLong, str(2).toDouble)
      })
      .assignAscendingTimestamps(_._2)

    val table: Table = tableEnv.fromDataStream(inputStream1, $"item ", $"bidtime".rowtime(), $"price")
    tableEnv.createTemporaryView("Bid", table)

    //     Table API 实现
    table.window(Slide.over(10.second()).every(5.second()).on($"bidtime").as("slide"))
      .groupBy($"slide")
      .select($"slide".start(), $"slide".end(), $"price".sum())
      .toRetractStream[Row]
      .print()
    env.execute()

    // SQL 实现方式一
    tableEnv.sqlQuery(
      """select window_start, window_end, sum(price)
        |from table(
        |HOP(TABLE Bid, DESCRIPTOR(bidtime), interval '5' second, interval '10' second )
        |) group by window_start, window_end
        |""".stripMargin).execute().print()

    // SQL 实现方式二
    tableEnv.sqlQuery(
      """
        |select HOP_START(bidtime, interval '5' second, interval '10' second),
        |HOP_END(bidtime, interval '5' second, interval '10' second),
        |SUM(price)
        |from Bid
        |group by HOP(bidtime, interval '5' second, interval '10' second)
        |""".stripMargin).execute().print()

  }
}