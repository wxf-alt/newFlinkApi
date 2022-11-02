package flinkTest.tableApi.rowBased

import bean.SensorReading
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row


/**
 * @Auther: wxf
 * @Date: 2022/10/25 10:48:19
 * @Description: A1_MapScalarFunctionTest   map中使用 标量函数
 * @Version 1.0.0
 */
object A1_MapScalarFunctionTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 环境配置
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val dataStream: DataStream[SensorReading] = inputStream.map(x => {
      val str: Array[String] = x.split(" ")
      SensorReading(str(0), str(1).toLong, str(2).toDouble)
    }) // 设置 waterMark
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading) = element.timestamp * 1000L
      })

    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'timestamp.rowtime as 'ts, 'temperature)

    val myMapFunction: MyMapFunction1 = new MyMapFunction1()
    val table1: Table = sensorTable.map(myMapFunction($"id")).as("s1")
    table1.toAppendStream[Row].print()

    env.execute("A1_MapScalarFunctionTest")
  }

  class MyMapFunction1 extends ScalarFunction {
    def eval(a: String): String = {
      "pre-" + a
    }
  }

}