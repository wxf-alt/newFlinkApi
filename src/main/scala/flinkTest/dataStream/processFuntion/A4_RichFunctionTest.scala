package flinkTest.dataStream.processFuntion

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/10/14 10:52:35
 * @Description: A4_RichFunctionTest
 * @Version 1.0.0
 */
object A4_RichFunctionTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(6)

    // 获取 source
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)

    inputStream.map(x => x).setParallelism(4)

    val mapStream: DataStream[String] = inputStream.map(new RichMapFunction[String, String]() {

      override def open(parameters: Configuration) = {
        println(s"${getRuntimeContext.getIndexOfThisSubtask} 子任务 进入 Map算子")
      }

      override def map(value: String) = {
        value
      }

      override def close() = println(s"${getRuntimeContext.getIndexOfThisSubtask} 子任务 退出 Map算子")
    })

    mapStream.print().setParallelism(3)
    env.executeAsync("A4_RichFunctionTest")
  }
}
