package flinkTest.tableApi.rowBased

import bean.SensorReading
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

/**
 * @Auther: wxf
 * @Date: 2022/10/25 14:19:52
 * @Description: A3_AggregateTest  aggregate中使用聚合函数
 * @Version 1.0.0
 */
object A3_AggregateTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
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

    val myMinMax: MyMinMax = new MyMinMax
    val table: Table = sensorTable
      .groupBy($"id")
      .aggregate(myMinMax($"temperature").as("min", "max"))
      .select($"id", $"min", $"max")
    table.printSchema()
    table.toRetractStream[Row].filter(_._1).print()

    env.execute("A3_AggregateTest")
  }

  case class MyMinMaxAcc(var min: Double, var max: Double)

  class MyMinMax extends AggregateFunction[(Double, Double), MyMinMaxAcc] {


    override def createAccumulator(): MyMinMaxAcc = MyMinMaxAcc(Double.MaxValue, Double.MinValue)

    override def getValue(accumulator: MyMinMaxAcc): (Double, Double) = (accumulator.min, accumulator.max)

    def accumulate(acc: MyMinMaxAcc, value: Double) = {
      if (value < acc.min) {
        acc.min = value
      }
      if (value > acc.max) {
        acc.max = value
      }
    }

  }

}