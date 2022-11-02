package flinkTest.tableApi.rowBased

import java.util

import bean.SensorReading
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/10/25 15:07:44
 * @Description: A4_FlatAggregateTest   使用表聚合函数
 * @Version 1.0.0
 */
object A4_FlatAggregateTest {
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

    val top2: Top2 = new Top2()
    val table: Table = sensorTable
      .groupBy($"id")
      .flatAggregate(top2($"temperature").as("v", "rank"))
      .select($"id", $"v", $"rank")

    table.printSchema()
    table.toRetractStream[Row].print()


    env.execute("A4_FlatAggregateTest")
  }

  case class MyMinMaxAcc(var first: Double, var second: Double)

  class Top2 extends TableAggregateFunction[(Double, Int), MyMinMaxAcc] {

    override def createAccumulator(): MyMinMaxAcc = MyMinMaxAcc(Double.MinValue, Double.MinValue)

    def accumulate(acc: MyMinMaxAcc, v: Double): Unit = {
      if (v > acc.first) {
        acc.second = acc.first
        acc.first = v
      } else if (v > acc.second) {
        acc.second = v
      }
    }

    def merge(acc: MyMinMaxAcc, its: java.lang.Iterable[MyMinMaxAcc]): Unit = {
      val iter: util.Iterator[MyMinMaxAcc] = its.iterator()
      while (iter.hasNext) {
        val top2: MyMinMaxAcc = iter.next()
        accumulate(acc, top2.first)
        accumulate(acc, top2.second)
      }
    }

    def emitValue(acc: MyMinMaxAcc, out: Collector[(Double, Int)]): Unit = {
      // 下发 value 与 rank
      if (acc.first != Double.MinValue) {
        out.collect((acc.first, 1))
      }
      if (acc.second != Double.MinValue) {
        out.collect((acc.second, 2))
      }
    }

  }

}