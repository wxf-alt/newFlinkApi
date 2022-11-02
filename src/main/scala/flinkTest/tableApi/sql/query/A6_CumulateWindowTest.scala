package flinkTest.tableApi.sql.query

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

/**
 * @Auther: wxf
 * @Date: 2022/10/27 17:54:38
 * @Description: A6_CumulateWindowTest
 * @Version 1.0.0
 */
object A6_CumulateWindowTest {
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


    tableEnv.sqlQuery(
      """
        |SELECT window_start, window_end, SUM(price)
        |  FROM TABLE(
        |    CUMULATE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '2' SECOND, INTERVAL '10' SECOND))
        |  GROUP BY window_start, window_end
        |""".stripMargin).execute().print()
  }
}