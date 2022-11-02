package flinkTest.tableApi.custom_function

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

/**
 * @Auther: wxf
 * @Date: 2022/11/1 19:42:19
 * @Description: SplitTableFunction
 * @Version 1.0.0
 */
object SplitTableFunction {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val table: Table = tableEnv.fromValues(DataTypes.ROW(
      DataTypes.FIELD("id", DataTypes.INT()),
      DataTypes.FIELD("name", DataTypes.STRING())),
      row(1, "ABC wrredfv refrr"),
      row(2, "ABCDE yrue yue")
    )

    //    // 调用 表函数 方式一
    //    val splitFunction: SplitFunction = new SplitFunction
    //    table.joinLateral(splitFunction($"name").as('word, 'length))
    //      .select($"id", $"name", $"word", $"length")
    //      .execute()
    //      .print()

    table.joinLateral(call(classOf[SplitFunction], $"name"))
      .select($"id", $"name", $"word", $"length")
      .execute()
      .print()

    table.joinLateral(call(classOf[SplitFunction], $"name") as("word", "length"))
      .select($"id", $"name", $"word", $"length")
      .execute()
      .print()

    // 注册函数
    tableEnv.createTemporarySystemFunction("SplitFunction", classOf[SplitFunction])

    table.joinLateral(call("SplitFunction", $"name") as("word", "length"))
      .select($"id", $"name", $"word", $"length")
      .execute()
      .print()

    // 在 SQL 里调用注册好的函数
    tableEnv.sqlQuery(s"SELECT id, word, length  FROM ${table}, LATERAL TABLE(SplitFunction(name))").execute().print()
    tableEnv.sqlQuery(s"SELECT id, word, length  FROM ${table} LEFT JOIN LATERAL TABLE(SplitFunction(name)) ON TRUE").execute().print()

    // 在 SQL 里重命名函数字段
    tableEnv.sqlQuery(
      "SELECT id, newWord, newLength " +
        s"FROM ${table} " +
        "LEFT JOIN LATERAL TABLE(SplitFunction(name)) AS T(newWord, newLength) ON TRUE").execute().print()
  }

  @FunctionHint(output = new DataTypeHint("ROW<word STRING, length INT>"))
  class SplitFunction extends TableFunction[Row] {

    def eval(str: String): Unit = {
      str.split(" ").foreach(x => collect(Row.of(x, Int.box(x.length))))
    }

    // 老版本 设置 输出数据类型 对应上面的第一个 使用方式实例化类使用
    //    override def getResultType: TypeInformation[Row] = {
    //      Types.ROW(Types.STRING(), Types.INT())
    //    }
  }

}