package flinkTest.tableApi.changelogStream

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.types.{Row, RowKind}

/**
 * @Auther: wxf
 * @Date: 2022/10/20 14:16:50
 * @Description: A1_FromChangelogStreamTest
 * @Version 1.0.0
 */
object A1_FromChangelogStreamTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val dataStream: DataStream[Row] = env.fromElements(
      Row.ofKind(RowKind.INSERT, "Alice", new Integer(12)),
      Row.ofKind(RowKind.INSERT, "Bob", new Integer(5)),
      Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", new Integer(12)),
      Row.ofKind(RowKind.UPDATE_AFTER, "Alice", new Integer(100))
    )(Types.ROW(Types.STRING, Types.INT))

    // 1.将Datastream解释为一个表
    val table: Table = tableEnv.fromChangelogStream(dataStream)
    table.printSchema()
    tableEnv.createTemporaryView("InputTable", table)
    tableEnv.executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0").print()
    //    +----+--------------------------------+-------------+
    //    | op |                           name |       score |
    //    +----+--------------------------------+-------------+
    //    | +I |                            Bob |           5 |
    //    | +I |                          Alice |          12 |
    //    | -D |                          Alice |          12 |
    //    | +I |                          Alice |         100 |
    //    +----+--------------------------------+-------------+

    // 2.将流解释为upsert流(不需要UPDATE_BEFORE)
    // create a changelog DataStream
    val dataStream1: DataStream[Row] = env.fromElements(
      Row.ofKind(RowKind.INSERT, "Alice", Int.box(12)),
      Row.ofKind(RowKind.INSERT, "Bob", Int.box(5)),
      Row.ofKind(RowKind.UPDATE_AFTER, "Alice", Int.box(100))
    )(Types.ROW(Types.STRING, Types.INT))

    // interpret the DataStream as a Table
    val table1 = tableEnv.fromChangelogStream(
      dataStream1,
      Schema.newBuilder().primaryKey("f0").build(),
      ChangelogMode.upsert())

    // register the table under a name and perform an aggregation
    tableEnv.createTemporaryView("InputTable1", table1)
    tableEnv.executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable1 GROUP BY f0").print()
    //    +----+--------------------------------+-------------+
    //    | op |                           name |       score |
    //    +----+--------------------------------+-------------+
    //    | +I |                            Bob |           5 |
    //    | +I |                          Alice |          12 |
    //    | -U |                          Alice |          12 |
    //    | +U |                          Alice |         100 |
    //    +----+--------------------------------+-------------+

  }
}