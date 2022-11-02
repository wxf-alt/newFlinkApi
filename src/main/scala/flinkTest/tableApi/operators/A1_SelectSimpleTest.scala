package flinkTest.tableApi.operators

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/**
 * @Auther: wxf
 * @Date: 2022/10/24 14:22:09
 * @Description: A1_SelectTest
 * @Version 1.0.0
 */
object A1_SelectSimpleTest {
  def main(args: Array[String]): Unit = {
    // Scala 的 Table API 通过引入 org.apache.flink.table.api._、org.apache.flink.api.scala._ 和 org.apache.flink.table.api.bridge.scala._（开启数据流的桥接支持）来使用。
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 环境配置
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)


    // 1.创建 数据表
    val table: Table = tableEnv.fromValues(row(1, "ABC"), row(2, "ABCDE"))
    table.printSchema()

    // 1.创建 数据表 指定列名
    val table1: Table = tableEnv.fromValues(DataTypes.ROW(
      DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
      DataTypes.FIELD("name", DataTypes.STRING())
    ), row(1, "ABC"), row(2, "ABCDE"))
    table1.printSchema()


    // 2.查询 select 操作
    table1.select($"id", $"name").toAppendStream[Row].print("$ --> ")
    table1.select('id, 'name).toAppendStream[Row].print("' -->")
    // 可以选择星号（*）作为通配符，select 表中的所有列。
    table1.select($"*").toAppendStream[Row].print("* -->")


    // 3.重命名字段 as 操作
    table1.as("code", "addr").printSchema()


    // 4.过滤 filter 操作
    table1.filter($"id" % 2 === 0).toAppendStream[Row].print("id % 2 = 0 : ")
    table1.filter($"id" % 2 !== 0).toAppendStream[Row].print("id % 2 != 0 : ")
    table1.where($"id" % 2 ===0).toAppendStream[Row].print("where id % 2 = 0 : ")


    env.execute("A1_SelectSimpleTest")
  }
}