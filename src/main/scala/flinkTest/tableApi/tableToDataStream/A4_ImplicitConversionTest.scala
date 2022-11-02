package flinkTest.tableApi.tableToDataStream

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/**
 * @Auther: wxf
 * @Date: 2022/10/20 17:40:37
 * @Description: A4_ImplicitConversionTest
 * @Version 1.0.0
 */
object A4_ImplicitConversionTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val dataStream: DataStream[(Int, String)] = env.fromElements((42, "hello"))

    // 在Datastream对象上隐式调用tochangelogTable)
    val table1: Table = dataStream.toTable(tableEnv)
    val table2: Table = dataStream.toChangelogTable(tableEnv)

    //强制隐式转换
    val dataStreamAgain1: DataStream[Row] = table2

    // 在Table对象上隐式更改 为 流
    val dataStreamAgain2: DataStream[Row] = table2.toAppendStream[Row]
    val dataStreamAgain3: DataStream[Row] = table1.toDataStream
  }
}
