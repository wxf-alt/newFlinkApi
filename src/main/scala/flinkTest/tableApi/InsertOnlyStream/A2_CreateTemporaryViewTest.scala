package flinkTest.tableApi.InsertOnlyStream

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

/**
 * @Auther: wxf
 * @Date: 2022/10/20 10:51:58
 * @Description: A2_CreateTemporaryViewTest
 * @Version 1.0.0
 */
object A2_CreateTemporaryViewTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val dataStream: DataStream[(Long, String)] = env.fromElements(
      (12L, "Alice"),
      (0L, "Bob")
    )

    // 1. 将Datastream注册为当前会话中的视图“MyView”，al7列自动派生
    tableEnv.createTemporaryView("MyView", dataStream)
    tableEnv.from("MyView").printSchema()
    //    (
    //      `_1` BIGINT NOT NULL,
    //      `_2` STRING
    //    )


    // 2. 在当前会话中将DataStream注册为“MyView”视图
    //  提供一个模式来调整类似于“fromDatastream”的列
    //  在本例中，派生的 NOT NULL 信息已被删除
    tableEnv.createTemporaryView("MyView1", dataStream,
      Schema.newBuilder()
        //        .columnByExpression("age", $"_1".cast(TypeInformation.of(classOf[Int])))
        //        .columnByExpression("name", $"_2".cast(TypeInformation.of(classOf[String])))
        .columnByExpression("age", $"_1".cast(DataTypes.INT()))
        .columnByExpression("name", $"_2".cast(DataTypes.STRING()))
        .build()
    )
    tableEnv.from("MyView1").printSchema()
    //    (
    //      `_1` BIGINT NOT NULL,
    //      `_2` STRING,
    //      `age` INT NOT NULL AS cast(_1, INT),
    //      `name` STRING AS cast(_2, STRING)
    //    )
    tableEnv.from("MyView1").dropColumns($"_1", $"_2").printSchema()
    //    (
    //      `age` INT NOT NULL,
    //      `name` STRING
    //    )

    // 3. 在本例中，派生的 NOT NULL 信息已被删除
    tableEnv.createTemporaryView("MyView2", dataStream,
      Schema.newBuilder()
        .column("_1", DataTypes.INT())
        .column("_2", DataTypes.STRING())
        .build()
    )
    tableEnv.from("MyView2").printSchema()
    //    (
    //      `_1` INT,
    //      `_2` STRING
    //    )

    // 4. 如果视图只涉及重命名列，则在创建视图之前使用 Table API
    tableEnv.createTemporaryView("MyView3",
      tableEnv.fromDataStream(dataStream).as("id", "name")
    )
    tableEnv.from("MyView3").printSchema()
    //    (
    //      `id` BIGINT NOT NULL,
    //      `name` STRING
    //    )

  }
}