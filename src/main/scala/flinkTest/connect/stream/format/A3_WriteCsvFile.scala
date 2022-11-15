package flinkTest.connect.stream.format

import java.time.Duration
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.java.io.CsvOutputFormat
import org.apache.flink.configuration.{Configuration, MemorySize, RestOptions}
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.connector.file.sink.compactor.{DecoderBasedReader, FileCompactStrategy, RecordWiseFileCompactor, SimpleStringDecoder}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.functions.sink.filesystem.{OutputFileConfig, StreamingFileSink}
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/11/2 21:44:06
 * @Description: A3_WriteCsvFile
 * @Version 1.0.0
 */
object A3_WriteCsvFile {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    env.enableCheckpointing(10000)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)

    val mapStream: DataStream[String] = inputStream.map(x => {
      val str: Array[String] = x.split("")
      str(0) + "," + str(1)
    })
    mapStream.print("mapStream：")

    val config: OutputFileConfig = OutputFileConfig
      .builder()
      .withPartPrefix("prefix")
      .withPartSuffix(".csv")
      .build()
    // 输出
    val fileSink: StreamingFileSink[String] = StreamingFileSink.forRowFormat(new Path("E:\\A_data\\4.测试数据\\flink数据\\csv输出"), new SimpleStringEncoder[String]("UTF-8"))
      .withRollingPolicy(DefaultRollingPolicy.builder()
        .withInactivityInterval(Duration.ofSeconds(10)) // 10秒 滚动一次
        .build())
      .withOutputFileConfig(config)
      .build()
    mapStream.addSink(fileSink)

    env.execute("A3_WriteCsvFile")
  }
}