package flinkTest.dataStream.state_checkpoint

import java.time.Duration

import bean.Sensor
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.io.FilePathFilter
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Auther: wxf
 * @Date: 2022/9/17 15:16:11
 * @Description:    验证 WaterMark 传递
 *              多个分区 取上游任务的最小 WaterMark
 * @Version 1.0.0
 */
object A3_WaterMarkTransmitTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    env.setParallelism(2)

    val path: String = "E:\\A_data\\3.code\\newFlinkApi\\src\\main\\resources\\sensor"
    val inputFormat: TextInputFormat = new TextInputFormat(new Path(path))
    // 每1秒 扫描一次路径
    val inputStream: DataStream[String] = env.readFile(inputFormat, path, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000, FilePathFilter.createDefaultFilter())

    val mapUnionStream: DataStream[Sensor] = inputStream.map(x => {
      val str: Array[String] = x.split(" ")
      Sensor(str(0), str(1).toLong * 1000, str(2).toDouble)
    }) // 设置 waterMark
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1))
        //        .withIdleness(Duration.ofMinutes(1L))
        .withTimestampAssigner(new SerializableTimestampAssigner[Sensor] {
          override def extractTimestamp(element: Sensor, recordTimestamp: Long): Long = element.timeStamp
        })
      )

    val keyStream: KeyedStream[Sensor, String] = mapUnionStream.keyBy(_.id)
    val aggregateStream: DataStream[(String, Double)] = keyStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
      //      .allowedLateness(Time.seconds(2))
      //      .sideOutputLateData(new OutputTag[Sensor]("late"))
      .aggregate(new AggregateFunction[Sensor, (String, Double), (String, Double)] {
        override def createAccumulator() = ("", 0.0D)

        override def add(value: Sensor, accumulator: (String, Double)) = (value.id, accumulator._2 + value.temperature)

        override def getResult(accumulator: (String, Double)) = accumulator

        override def merge(a: (String, Double), b: (String, Double)) = (a._1, a._2 + b._2)
      })

    aggregateStream.print("aggregateStream：").setParallelism(1)

    env.execute("a13_stateBackendTest")
  }
}
