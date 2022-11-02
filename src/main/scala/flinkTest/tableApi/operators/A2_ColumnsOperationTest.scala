package flinkTest.tableApi.operators

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row

/**
 * @Auther: wxf
 * @Date: 2022/10/24 15:22:31
 * @Description: A2_ColumnsOperationTest
 * @Version 1.0.0
 */
object A2_ColumnsOperationTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 环境配置
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 1.创建 数据表 指定列名
    val table: Table = tableEnv.fromValues(DataTypes.ROW(
      DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
      DataTypes.FIELD("name", DataTypes.STRING())
    ), row(1, "ABC"), row(2, "ABCDE"))

    // 1.添加字段 如果所添加的字段已经存在，将抛出异常
    val addColumnTable1: Table = table.addColumns(concat($"name", "Sunny") as "add_column")
    addColumnTable1.printSchema()
    //    addColumnTable1.toAppendStream[Row].print()

    // 2.添加字段  如果添加的列名称和已存在的列名称相同，则已存在的字段将被替换。 此外，如果添加的字段里面有重复的字段名，则会使用最后一个字段。
    val addColumnTable2: Table = addColumnTable1.addOrReplaceColumns(concat($"add_column", "Happy") as "add_column")
    addColumnTable2.printSchema()
    //    addColumnTable2.toAppendStream[Row].print()

    // 3.删除字段
    addColumnTable1.dropColumns($"add_column").printSchema()

    // 4.重命名字段
    addColumnTable1.renameColumns($"add_column".as("renameColumn")).printSchema()


    env.execute("A2_ColumnsOperationTest")
  }
}