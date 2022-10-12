package flinkTest.dataStream.partitioner

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/10/10 19:50:09
 * @Description: A5_BroadcastTest  数据广播到 下游的所有并行实例中
 * @Version 1.0.0
 */
object A5_BroadcastTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8082)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(5)

    val inputStream1: DataStream[String] = env.socketTextStream("localhost", 6666)
    val globalStream: DataStream[String] = inputStream1.broadcast

    globalStream.print()
    env.executeAsync("A5_BroadcastTest")
  }
}
