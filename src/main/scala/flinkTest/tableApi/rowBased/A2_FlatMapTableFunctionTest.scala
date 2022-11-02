package flinkTest.tableApi.rowBased

import bean.SensorReading
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

/**
 * @Auther: wxf
 * @Date: 2022/10/25 11:54:48
 * @Description: A2_FlatMapTableFunctionTest  flatMap中使用表函数
 * @Version 1.0.0
 */
object A2_FlatMapTableFunctionTest {
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

    val myFlatMapFunction1: MyFlatMapFunction1 = new MyFlatMapFunction1
    sensorTable.flatMap(myFlatMapFunction1($"id")).as("a", "b").toAppendStream[Row].print()

    env.execute("A2_FlatMapTableFunctionTest")
  }

  class MyFlatMapFunction1 extends TableFunction[(String, Int)] {
    def eval(str: String): Unit = {
      if (str.contains("#")) {
        str.split("#").foreach(x => collect(x, x.length))
      }
    }
  }

  class MyFlatMapFunction extends TableFunction[Row] {
    def eval(str: String): Unit = {
      if (str.contains("#")) {
        str.split("#").foreach({ s =>
          val row: Row = new Row(2)
          row.setField(0, s)
          row.setField(1, s.length)
          collect(row)
        })
      }
    }

    override def getResultType: TypeInformation[Row] = {
      Types.ROW(Types.STRING, Types.INT)
    }
  }

}