package flinkTest.tableApi.custom_function

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.{FunctionContext, ScalarFunction}
import org.apache.flink.types.Row

/**
 * @Auther: wxf
 * @Date: 2022/11/1 18:11:17
 * @Description: HashScalarFunction
 * @Version 1.0.0
 */
object HashScalarFunction {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //    // 使用 VALUES 子句 生成测试数据
    //    val table: Table = tableEnv.sqlQuery(
    //      """
    //        |SELECT order_id, price FROM (VALUES(1,2.0),(2,3.1)) AS t(order_id,price)
    //        |""".stripMargin)

    //    val table: Table = tableEnv.fromValues(row(1, "ABC"), row(2, "ABCDE"))

    val table: Table = tableEnv.fromValues(DataTypes.ROW(
      DataTypes.FIELD("id", DataTypes.INT()),
      DataTypes.FIELD("name", DataTypes.STRING())),
      row(1, "ABC"),
      row(2, "ABCDE")
    )

    // 使用 标量函数 方式一：
    val hashFunction: HashFunction = new HashFunction
    table.select($"id", $"name", hashFunction($"name")).execute().print()

    // 方式二：在 Table API 里不经注册直接“内联”调用函数
    // 使用 toDataStream 必须 调用 env.execute
    table.select($"id", $"name", call(classOf[HashFunction], $"name")).toDataStream.print("2：")

    // 方式三：注册函数 使用  不仅可以在 TableAPI中使用,还可以在SQL中使用
    tableEnv.createFunction("HashFunction", classOf[HashFunction])
    table.select($"id", $"name", call("HashFunction", $"name")).toDataStream.print("3：")
    tableEnv.sqlQuery(s"select id, name, HashFunction(name) from ${table}").toDataStream.print("4：")
    tableEnv.executeSql(s"select id, name, HashFunction(name) from ${table}").print()

    env.execute("HashScalarFunction")

  }

  class HashFunction extends ScalarFunction {

    private var factor: Int = 0

    override def open(context: FunctionContext): Unit = {
      // 获取 环境中的配置
      factor = context.getJobParameter("hashcode_factor", "12").toInt
    }

    def eval(str: String): Int = {
      str.hashCode * factor
    }

  }

}