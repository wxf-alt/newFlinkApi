package flinkTest.tableApi.tableToDataStream

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/**
 * @Auther: wxf
 * @Date: 2022/10/19 14:27:22
 * @Description: A2_ToChangelogStreamTest  将Table转换为Stream 仅变更日志Table可转流
 * @Version 1.0.0
 */
object A2_ToChangelogStreamTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    // 设置 执行模式为 批处理
//    env.setRuntimeMode(RuntimeExecutionMode.BATCH)

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val dataStream: DataStream[(String, Int)] = env.fromElements(("Alice",12),("Bob",10),("Alice",100))

    // 根据流生成 Table
    val inputTable: Table = tableEnv.fromDataStream(dataStream).as("name", "score")
    //    tableEnv.createTemporaryView("InputTable", dataStream, 'name, 'score)
    tableEnv.createTemporaryView("InputTable", inputTable)

    // 执行 SQL
    val resultTable: Table = tableEnv.sqlQuery("SELECT name, SUM(score) FROM InputTable GROUP BY name")

    val resultStream : DataStream[Row] = tableEnv.toChangelogStream(resultTable)

    resultTable.execute().print()
    resultStream.print()

    env.execute()
  }
}