package flinkTest.tableApi.operators

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.{DataTypes, Table, row}
import org.apache.flink.types.Row

/**
 * @Auther: wxf
 * @Date: 2022/10/24 19:03:17
 * @Description: A5_TwoStreamTest
 * @Version 1.0.0
 */
object A5_TwoStreamTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 环境配置
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, EnvironmentSettings.inBatchMode())

    // 1.创建 数据表 指定列名
    val table: Table = tableEnv.fromValues(DataTypes.ROW(
      DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
      DataTypes.FIELD("name", DataTypes.STRING())
    ), row(1, "ABC"),
      row(1, "ABC"),
      row(2, "ABCDE"),
      row(3, "ABCDE"),
      row(3, "ABCDE")
    )
    val table1: Table = tableEnv.fromValues(DataTypes.ROW(
      DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
      DataTypes.FIELD("name", DataTypes.STRING())
    ), row(2, "ABC"),
      row(2, "ABCDE"),
      row(3, "ABCDE"),
      row(3, "ABCDE")
    )

    //    // union 合并两条流 去重
    //    //  只针对 批处理 使用
    table.union(table1).toAppendStream[Row].print("union")
    //    union> +I[1.00, ABC]
    //    union> +I[2.00, ABCDE]
    //    union> +I[3.00, ABCDE]
    //    union> +I[2.00, ABC]
    //    // union 合并两条流 不会去重  只能使用 distinct 进一步操作
    table.unionAll(table1).toAppendStream[Row].print("unionAll")
    //    unionAll> +I[1.00, ABC]
    //    unionAll> +I[1.00, ABC]
    //    unionAll> +I[2.00, ABCDE]
    //    unionAll> +I[2.00, ABCDE]
    //    unionAll> +I[3.00, ABCDE]
    //    unionAll> +I[3.00, ABCDE]
    //    unionAll> +I[3.00, ABCDE]
    //    unionAll> +I[3.00, ABCDE]
    //    unionAll> +I[2.00, ABC]
    table.unionAll(table1).distinct().toAppendStream[Row].print("unionAll distinct")
    //    unionAll distinct> +I[1.00, ABC]
    //    unionAll distinct> +I[2.00, ABCDE]
    //    unionAll distinct> +I[3.00, ABCDE]
    //    unionAll distinct> +I[2.00, ABC]

    //    //    // 交集   返回结果没有重复数据
    table.intersect(table1).toAppendStream[Row].print("intersect")
    //    intersect> +I[2.00, ABCDE]
    //    intersect> +I[3.00, ABCDE]
    //    //     // 交集   返回结果带有重复数据  重复的条数 与两个流中出现的条数 相同
    table.intersectAll(table1).toAppendStream[Row].print("intersectAll")
    //    intersectAll> +I[2.00, ABCDE]
    //    intersectAll> +I[3.00, ABCDE]
    //    intersectAll> +I[3.00, ABCDE]

    // 返回左表中存在且右表中不存在的记录  返回结果 去重
    table.minus(table1).toAppendStream[Row].print("minus")
    //    minus> +I[1.00, ABC]
    // 返回左表中存在且右表中不存在的记录  返回结果 不去重
    table.minusAll(table1).toAppendStream[Row].print("minusAll")
    //    minusAll> +I[1.00, ABC]
    //    minusAll> +I[1.00, ABC]


    // in 子句 如果表达式的值存在于给定表的子查询中，那么 In 子句返回 true。子查询表必须由一列组成。这个列必须与表达式具有相同的数据类型
    table.select($"id", $"name").where($"id".in(table1.select($"id"))).toAppendStream[Row].print("in 子句")
    //    in 子句> +I[2.00, ABCDE]
    //    in 子句> +I[3.00, ABCDE]
    //    in 子句> +I[3.00, ABCDE]

    env.execute("A5_TwoStreamTest")

  }
}