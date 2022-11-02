package flinkTest.tableApi.operators

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row

/**
 * @Auther: wxf
 * @Date: 2022/10/24 15:38:29
 * @Description: A3_AggregationTest
 * @Version 1.0.0
 */
object A3_AggregationTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 环境配置
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 1.创建 数据表 指定列名
    val table: Table = tableEnv.fromValues(DataTypes.ROW(
      DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
      DataTypes.FIELD("name", DataTypes.STRING())
    ), row(1, "ABC"),
      row(2, "ABCDE"),
      row(3, "ABCDE32"),
      row(5, "ABCDEED"),
      row(4, "ABCDEAW"),
      row(2, "ABCDE"),
      row(1, "ABCDE"),
      row(5, "ABCDE"),
      row(2, "ABCDEQW")
    )

    // 分组 求count
    table.groupBy($"id")
      .select($"id", $"name".count().distinct() as "num")  // 去重实现
      //      .select($"id", $"name".count() as "num")
      .toRetractStream[Row]
      .filter(_._1)
      .print()


    env.execute("A3_AggregationTest")
  }
}