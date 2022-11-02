package flinkTest.tableApi

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * @Auther: wxf
 * @Date: 2022/10/18 16:16:00
 * @Description: A1_CreateTableEnv
 * @Version 1.0.0
 */
object A1_CreateTableEnv {
  def main(args: Array[String]): Unit = {

    // 创建 流式 TableAPI 执行环境
    // 1.利用 StreamExecutionEnvironment 创建
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 2.使用 EnvironmentSettings
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()
    val tableEnv2: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)


    // 创建 批处理 TableAPI 执行环境
    val bbSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .inBatchMode()
      .build()
    val bbTableEnv: TableEnvironment = TableEnvironment.create(bbSettings)

  }
}