package flinkTest.tableApi.statementSet

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.{StreamStatementSet, StreamTableEnvironment}
import org.apache.flink.streaming.api.functions.sink.DiscardingSink

/**
 * @Auther: wxf
 * @Date: 2022/10/20 15:45:28
 * @Description: A1_StatementSetAddTest
 *               将表程序添加到一个作业中的数据流 API 程序中
 * @Version 1.0.0
 */
object A1_StatementSetAddTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 创建 作业流
    val statementSet: StreamStatementSet = tableEnv.createStatementSet()

    // 获取 数据 连接 datagen数据源
    val sourceDescriptor: TableDescriptor = TableDescriptor.forConnector("datagen")
      .option("number-of-rows", "3")
      .schema(Schema.newBuilder()
        .column("myCol", "int")
        .column("myOtherCol", "boolean")
        .build()
      ).build()
    // 获取 输出数据源
    val sinkDescriptor: TableDescriptor = TableDescriptor.forConnector("print").build()
    val tableFromSource: Table = tableEnv.from(sourceDescriptor)
    // 添加作业 到 作业流  添加一个纯粹的Table API管道
    statementSet.addInsert(sinkDescriptor, tableFromSource)

    val dataStream: DataStream[Int] = env.fromElements(1, 2, 3)
    val tableFromStream: Table = tableEnv.fromDataStream(dataStream)
    // 添加一个纯粹的Table API管道
    statementSet.addInsert(sinkDescriptor, tableFromStream)

    // 将两个管道连接到StreamExecutionEnvironment(调用此方法将清除语句集)
    // 不写 就会报：No operators defined in streaming topology. Cannot execute.
    statementSet.attachAsDataStream()

    // 定义其他DataStream API部分
    // 忽略所有输入元素
    env.fromElements(4, 5, 6).addSink(new DiscardingSink[Int]())

    // 现在使用DataStream API来提交管道
    env.execute()
  }
}