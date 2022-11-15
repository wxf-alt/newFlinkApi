package flinkTest.connect.tale

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._

/**
 * @Auther: wxf
 * @Date: 2022/11/14 17:38:22
 * @Description: DataGenTest
 * @Version 1.0.0
 */
object DataGenTest {
  def main(args: Array[String]): Unit = {

    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    tableEnv.executeSql(
      """
        |CREATE TABLE Orders (
        |   id INT,
        |   user_id        BIGINT,
        |   total_amount   DOUBLE,
        |   user_action_time AS PROCTIME()
        |) WITH(
        | 'connector'='datagen',
        | 'rows-per-second'='5',
        | 'fields.id.kind'='sequence' ,
        | 'fields.id.start'='1',
        | 'fields.id.end'='100',
        | 'fields.user_id.kind'='random',
        | 'fields.user_id.min'='1',
        | 'fields.user_id.max'='100',
        | 'fields.total_amount.kind'='random',
        | 'fields.total_amount.min'='1',
        | 'fields.total_amount.max'='100'
        | )
        |""".stripMargin)

    tableEnv.executeSql( "select * from Orders").print()
  }
}