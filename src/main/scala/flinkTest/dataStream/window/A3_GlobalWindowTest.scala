package flinkTest.dataStream.window

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows

/**
 * @Auther: wxf
 * @Date: 2022/10/11 16:23:05
 * @Description: A3_GlobalWindowTest
 * @Version 1.0.0
 */
object A3_GlobalWindowTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val keyedStream: KeyedStream[(String, Int), String] = inputStream.map((_, 1)).keyBy(_._1)

    // 不指定 自定义的 trigger 不会触发窗口函数计算
    val windowStream: DataStream[(String, Int)] = keyedStream.window(GlobalWindows.create())
      .reduce((x, y) => (x._1, x._2 + y._2))

    windowStream.print()
    env.executeAsync("A3_GlobalWindowTest")
  }
}
