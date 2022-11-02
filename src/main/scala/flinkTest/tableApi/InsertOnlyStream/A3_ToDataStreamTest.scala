package flinkTest.tableApi.InsertOnlyStream

import flinkTest.tableApi.InsertOnlyStream.A1_FromDataStreamTest.User
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row

/**
 * @Auther: wxf
 * @Date: 2022/10/20 11:27:23
 * @Description: A3_ToDataStreamTest
 * @Version 1.0.0
 */
object A3_ToDataStreamTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 执行sql 创建数据源
    tableEnv.executeSql(
      """
        |CREATE TABLE GeneratedTable (
        |    name STRING,
        |    score INT,
        |    event_time AS localtimestamp,
        |    WATERMARK FOR event_time AS event_time
        |  )
        |  WITH (
        |  'connector' = 'datagen',
        |  'rows-per-second' = '20'
        |  )
        |""".stripMargin)

    // 读取 数据源
    val table: Table = tableEnv.from("GeneratedTable")

    // 1.使用到Row实例的默认转换
    //  因为'event_time'是一个单独的行时间属性，它被插入到DataStream元数据中，并传播水印
    val dataStream: DataStream[Row] = tableEnv.toDataStream(table)


    // 2.从类'User'中提取数据类型，计划器对字段重新排序并在可能的地方插入隐式强制转换，
    //  以将内部数据结构转换为所需的结构化类型因为'event_time'是一个单独的行时间属性，它被插入到Datastream元数据中并传播水印
    val dataStream1: DataStream[User] = tableEnv.toDataStream(table, classOf[User])

    // 3.数据类型可以像上面那样反射式地提取，也可以显式地定义
    val dataStream3: DataStream[Nothing] = tableEnv.toDataStream(table, DataTypes.STRUCTURED(
      classOf[User],
      DataTypes.FIELD("name", DataTypes.STRING()),
      DataTypes.FIELD("score", DataTypes.INT()),
      DataTypes.FIELD("event_time", DataTypes.TIMESTAMP_LTZ(3)))
    )

  }
}
