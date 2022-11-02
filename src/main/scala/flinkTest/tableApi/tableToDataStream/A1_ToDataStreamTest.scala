package flinkTest.tableApi.tableToDataStream

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/**
 * @Auther: wxf
 * @Date: 2022/10/18 19:49:53
 * @Description: A1_ToDataStreamTest  将Table转换为Stream 仅插入更新Table可转流
 * @Version 1.0.0
 */
object A1_ToDataStreamTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 创建数据流
    val dataStream: DataStream[String] = env.fromElements("Alice", "Bob", "John")

    val inputTable: Table = tableEnv.fromDataStream(dataStream)
    // 使用表环境 创建虚拟表(视图)
    tableEnv.createTemporaryView("InputTable", inputTable)
    // 查询 虚拟表  列名 自动生成
    val resultTable: Table = tableEnv.sqlQuery("SELECT UPPER(f0) FROM InputTable")

    // 将 Table 转成 dataStream
    val resultStream: DataStream[Row] = tableEnv.toDataStream(resultTable)
    val resultStream2: DataStream[Row] = resultTable.toDataStream
    val resultStream3: DataStream[Row] = resultTable.toAppendStream
    //    val resultStream: DataStream[Row] = tableEnv.toChangelogStream(resultTable)

    resultStream.print()
    resultStream2.print()
    resultStream3.print()
    env.execute("A1_ToDataStreamTest")
  }
}