package flinkTest.tableApi.tableToDataStream

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

/**
 * @Auther: wxf
 * @Date: 2022/10/19 16:04:19
 * @Description: A3_BatchTableConnectDatagen  datagen数据源
 * @Version 1.0.0
 */
object A3_BatchTableConnectDatagen {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //    env.setRuntimeMode(RuntimeExecutionMode.BATCH)
    //    StreamTableEnvironment.create(env)

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, EnvironmentSettings.inBatchMode())
    val table: Table = tableEnv.from(TableDescriptor.forConnector("datagen")
      .option("number-of-rows", "10")
      .schema(
        Schema.newBuilder()
          .column("uid", DataTypes.TINYINT())
          .column("payload", DataTypes.STRING())
          .build())
      .build())
    tableEnv.toDataStream(table)
      .keyBy(r => r.getFieldAs[Byte]("uid"))
      .map(r => "My custom operator: " + r.getFieldAs[String]("payload"))
      .executeAndCollect()
      .foreach(println)

    // 批处理 不需要写 下面这行代码
    //    env.execute("A3_BatchTableConnectDatagen")
  }
}
