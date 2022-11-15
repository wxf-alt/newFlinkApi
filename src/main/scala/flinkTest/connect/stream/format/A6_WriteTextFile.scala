package flinkTest.connect.stream.format

import java.time.Duration

import flinkTest.connect.stream.format.A5_WriteParquetFile.MySource
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala._


/**
 * @Auther: wxf
 * @Date: 2022/11/3 21:18:45
 * @Description: A6_ReadTextFile
 * @Version 1.0.0
 */
object A6_WriteTextFile {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    env.enableCheckpointing(120000)

    val inputStream: DataStream[String] = env.addSource(new MySource())

    val outputFileConfig: OutputFileConfig = OutputFileConfig.builder().withPartPrefix("prefix").withPartSuffix(".txt").build()
    val fileSink: FileSink[String] = FileSink.forRowFormat(new Path("E:\\A_data\\4.测试数据\\flink数据\\Text输出"), new SimpleStringEncoder[String]())
      .withOutputFileConfig(outputFileConfig)
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withInactivityInterval(Duration.ofMinutes(10))
          .withRolloverInterval(Duration.ofMillis(5))
          .build())
      .build()

    inputStream.sinkTo(fileSink)

    env.execute("A6_ReadTextFile")
  }

}