package flinkTest.dataStream.processFuntion

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Auther: wxf
 * @Date: 2022/10/17 19:33:46
 * @Description: A5_OutputStreamTest  侧输出
 * @Version 1.0.0
 */
object A5_OutputStreamTest {
  def main(args: Array[String]): Unit = {
    //生成配置对象
    val conf: Configuration = new Configuration()
    conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    val outputTag: OutputTag[(String, String)] = OutputTag[(String, String)]("side-output")
    val date: Date = new Date()
    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")

    // 获取 source
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val outputStream: DataStream[String] = inputStream.process(new ProcessFunction[String, String] {

      override def processElement(value: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]) = {
        if (value == "aa") {
          val l: Long = System.currentTimeMillis()
          date.setTime(l)
          ctx.output(outputTag, (value, simpleDateFormat.format(date)))
        } else {
          out.collect(value)
        }
      }
    })

    outputStream.print("outputStream：")
    outputStream.getSideOutput(outputTag).print("outputTag：")
    env.executeAsync("A5_OutputStreamTest")
  }
}