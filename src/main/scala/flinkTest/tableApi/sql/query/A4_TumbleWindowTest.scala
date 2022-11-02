package flinkTest.tableApi.sql.query

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/10/27 10:56:49
 * @Description: A4_TumbleWindowTest
 * @Version 1.0.0
 */
object A4_TumbleWindowTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val inputStream1: DataStream[(String, Long, Double)] = env.socketTextStream("localhost", 6666)
      .map(x => {
        val str: Array[String] = x.split(" ")
        (str(0), str(1).toLong, str(2).toDouble)
      })
      .assignAscendingTimestamps(_._2)

    //     DataStream 实现
    val result: DataStream[(String, String, Double)] = inputStream1.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
      .process(new ProcessAllWindowFunction[(String, Long, Double), (String, String, Double), TimeWindow] {

        private val date: Date = new Date()
        private val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

        override def process(context: Context, elements: Iterable[(String, Long, Double)], out: Collector[(String, String, Double)]): Unit = {
          var sum: Double = 0
          val iterable: Iterable[(String, Long, Double)] = elements.toIterable
          for (elem <- iterable) {
            sum = elem._3 + sum
          }
          val start: Long = context.window.getStart
          val end: Long = context.window.getEnd
          date.setTime(start)
          val startWindow: String = simpleDateFormat.format(date)
          date.setTime(end)
          val endWindow: String = simpleDateFormat.format(date)
          out.collect((startWindow, endWindow, sum))
        }
      })
    result.print()

    val table: Table = tableEnv.fromDataStream(inputStream1, $"item ", $"bidtime".rowtime(), $"price")
    tableEnv.createTemporaryView("Bid", table)

    // 方式一  在 tumbling 窗口表上应用聚合
    tableEnv.sqlQuery(
      """SELECT window_start, window_end, SUM(price) FROM TABLE(
        |   TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), interval '5' second)
        |) GROUP BY window_start, window_end
        |""".stripMargin).execute().print()

    // 方式二 实现
    tableEnv.sqlQuery(
      """SELECT tumble_start(bidtime,interval '5' second), tumble_end(bidtime,interval '5' second), SUM(price) FROM Bid GROUP BY TUMBLE(bidtime, interval '5' second)
        |""".stripMargin).execute().print()

    env.execute()
  }
}