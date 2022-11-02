package flinkTest.tableApi.changelogStream

import java.time.Instant

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.data.StringData
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/10/20 14:55:10
 * @Description: A2_ToChangelogStreamTest
 * @Version 1.0.0
 */
object A2_ToChangelogStreamTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 1.以最简单、最通用的方式转换到DataStream(没有事件时间)
    val simpleTable: Table = tableEnv
      .fromValues(row("Alice", 12), row("Alice", 2), row("Bob", 12))
      .as("name", "score")
      .groupBy($"name")
      .select($"name", $"score".sum())
    tableEnv.toChangelogStream(simpleTable)
      .executeAndCollect()
      .foreach(println)
    //    +I[Bob, 12]
    //    +I[Alice, 12]
    //    -U[Alice, 12]
    //    +U[Alice, 14]

    // 2.以最简单和最通用的方式转换到Datastream(使用事件时间)
    tableEnv.executeSql(
      """
             CREATE TABLE GeneratedTable (
             name STRING,
             score INT,
             event_time TIMESTAMP_LTZ(3),
             WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
             )
             WITH ('connector'='datagen')
             """)
    val table: Table = tableEnv.from("GeneratedTable")
    val dataStream1: DataStream[Row] = tableEnv.toChangelogStream(table)
    dataStream1.process(new ProcessFunction[Row, Unit] {
      override def processElement(row: Row, ctx: ProcessFunction[Row, Unit]#Context, out: Collector[Unit]): Unit = {
        // prints: [name, score, event_time]
        println(row.getFieldNames(true))
        // timestamp exists twice
        assert(ctx.timestamp() == row.getFieldAs[Instant]("event_time").toEpochMilli)
      }
    })
    env.execute()

    // 3.转换到DataStream，但将time属性作为元数据列写出来，这意味着它不再是物理模式的一部分
    val dataStream2: DataStream[Row] = tableEnv.toChangelogStream(
      table,
      Schema.newBuilder()
        .column("name", "STRING")
        .column("score", "INT")
        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
        .build())

    dataStream2.process(new ProcessFunction[Row, Unit] {
      override def processElement(row: Row, ctx: ProcessFunction[Row, Unit]#Context, out: Collector[Unit]): Unit = {
        // prints: [name, score]
        println(row.getFieldNames(true))
        // timestamp exists once
        println(ctx.timestamp())
      }
    })
    env.execute()

    // 4.对于高级用户，也可以使用更多的内部数据结构来提高效率注意，这里提到这一点只是为了完整性，
    // 因为使用内部数据结构增加了复杂性和额外的类型处理然而，
    // 将TIMESTAMP_LTZ列转换为'Long'或将STRING转换为'byte[]'可能比较方便，如果需要，结构化类型也可以表示为Row'
    val dataStream4: DataStream[Row] = tableEnv.toChangelogStream(
      table,
      Schema.newBuilder()
        .column( "name", DataTypes.STRING().bridgedTo(classOf[StringData]))
        .column( "score", DataTypes.INT())
        .column( "event_time", DataTypes.TIMESTAMP_LTZ(3).bridgedTo(classOf[Long]))
    .build())

  }
}
