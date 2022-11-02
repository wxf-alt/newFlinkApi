package flinkTest.tableApi.InsertOnlyStream

import java.time.Instant

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * @Auther: wxf
 * @Date: 2022/10/20 09:18:15
 * @Description: A1_FromDataStreamTest
 * @Version 1.0.0
 */
object A1_FromDataStreamTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 创建数据流
    val dataStream: DataStream[User] = env.fromElements(
      User("Alice", 4, Instant.ofEpochMilli(1000)),
      User("Bob", 6, Instant.ofEpochMilli(1001)),
      User("Alice", 10, Instant.ofEpochMilli(1002)))

    // 自动派生所有物理列
    val table1: Table = tableEnv.fromDataStream(dataStream)
    table1.printSchema()
    //    (
    //      `name` STRING,
    //      `score` INT NOT NULL,
    //      `event_time` TIMESTAMP_LTZ(9)
    //    )

    // 自动派生所有物理列
    //    但是添加计算列(在本例中用于创建 proctime 属性列)
    val table2: Table = tableEnv.fromDataStream(dataStream,
      // 设置 处理时间
      Schema.newBuilder().columnByExpression("proctime", "PROCTIME()").build()
    )
    table2.printSchema()
    //    (
    //      `name` STRING,
    //      `score` INT NOT NULL,
    //      `event_time` TIMESTAMP_LTZ(9),
    //      `proctime` TIMESTAMP_LTZ(3) NOT NULL *PROCTIME* AS PROCTIME()
    //    )

    // 自动派生所有物理列
    //    但是添加计算列(在本例中用于创建 rowtime属性列 ) 和自定义水印策略
    val table3: Table = tableEnv.fromDataStream(dataStream,
      Schema.newBuilder()
        .columnByExpression("rowtime", "cast(event_time as TIMESTAMP_LTZ(3))")
        //        .watermark("rowtime", "rowtime - interval 10 second")  大小写都可以
        .watermark("rowtime", "rowtime - INTERVAL '10' SECOND")
        .build()
    )
    table3.printSchema()
    //    (
    //      `name` STRING,
    //      `score` INT NOT NULL,
    //      `event_time` TIMESTAMP_LTZ(9),
    //      `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* AS cast(event_time as TIMESTAMP_LTZ(3)),
    //      WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS rowtime - INTERVAL '10' SECOND
    //    )

    // 自动派生所有物理列
    //    但是访问流记录的时间戳来创建行时间属性列也依赖于Datastream API中生成的水印
    //    我们假设之前已经为'dataStream'定义了水印策略(不属于本示例)。
    val table4: Table = tableEnv.fromDataStream(dataStream,
      Schema.newBuilder()
        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
        .watermark("rowtime", "SOURCE_WATERMARK()")
        .build()
    )
    table4.printSchema()
    //    (
    //      `name` STRING,
    //      `score` INT NOT NULL,
    //      `event_time` TIMESTAMP_LTZ(9),
    //      `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* METADATA,
    //      WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS SOURCE_WATERMARK()
    //    )

    // 手动定义物理列在这个例子中，
    //    我们可以将时间戳的默认精度从9降低到3
    val table5: Table = tableEnv.fromDataStream(
      dataStream,
      Schema.newBuilder()
        .column("name", "STRING")
        .column("score", "INT")
        .column("event_time", "TIMESTAMP_LTZ(3)")
        .watermark("event_time", "SOURCE_WATERMARK()")
        .build())
    table5.printSchema()
    //    (
    //      `name` STRING,
    //      `score` INT,
    //      `event_time` TIMESTAMP_LTZ(3) *ROWTIME*,
    //      WATERMARK FOR `event_time`: TIMESTAMP_LTZ(3) AS SOURCE_WATERMARK()
    //    )

  }

  case class User(name: String, score: java.lang.Integer, event_time: java.time.Instant)

}
