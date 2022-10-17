package flinkTableApi.test

import java.time.Duration
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.{JobExecutionResult, RuntimeExecutionMode}
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.core.execution.JobClient
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.{OutputFileConfig, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala._


/**
 * @Auther: wxf
 * @Date: 2022/9/7 20:30:08
 * @Description: getEnvAndOutText   获取 运行环境 并输出文件
 * @Version 1.0.0
 */
object A1_GetEnvAndOutText {
  def main(args: Array[String]): Unit = {

    //    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()

    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8888)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val path: String = ClassLoader.getSystemResource("log4j.properties").getPath
    println(path)
    val text: DataStream[String] = env.readTextFile(path)
    val mapStream: DataStream[String] = text.map(x => {
      val str: Array[String] = x.split("=")
      str(0)
    }).setParallelism(4)

    val outputFileConfig: OutputFileConfig = OutputFileConfig.builder().withPartPrefix("prefix").withPartSuffix(".txt").build()
    // 输出
    val fileSink: StreamingFileSink[String] = StreamingFileSink.forRowFormat(new Path("./SinkOut1"), new SimpleStringEncoder[String]("UTF-8"))
      .withOutputFileConfig(outputFileConfig)
      .withRollingPolicy(DefaultRollingPolicy.builder()
        //        .withInactivityInterval(TimeUnit.MINUTES.toMillis(1)) // 1f分钟 滚动一次
        .withInactivityInterval(Duration.ofMinutes(1)) // 1分钟 滚动一次
        .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
        .withMaxPartSize(1024 * 1024 * 1024)
        .build())
      .build()
    mapStream.addSink(fileSink)

    //    mapStream.writeAsText("E:\\A_data\\3.code\\newFlinkApi\\src\\main\\resources\\SinkOut2", WriteMode.OVERWRITE)

    // 同步提交
    val result: JobExecutionResult = env.execute("getExecutionEnvironment")
    println("Runtime：" + result.getNetRuntime(TimeUnit.MILLISECONDS))
    println("Runtime：" + result.getNetRuntime())

    //    // 异步提交   不需要等待程序运行完成
    //    val jobClient: JobClient = env.executeAsync("getExecutionEnvironment")
    //
    //    for (elem <- 1 to 10) {
    //      println(elem)
    //    }
    //
    //    val result: JobExecutionResult = jobClient.getJobExecutionResult.get()
    //    println("Runtime：" + result.getNetRuntime(TimeUnit.MILLISECONDS))
    //    println("Runtime：" + result.getNetRuntime())

  }
}
