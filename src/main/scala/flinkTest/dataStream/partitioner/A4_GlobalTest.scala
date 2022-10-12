package flinkTest.dataStream.partitioner

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/10/10 19:47:47
 * @Description: A4_GlobalTest  将数据都划分到 下游的第一个实例中
 * @Version 1.0.0
 */
object A4_GlobalTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(5)

    val inputStream1: DataStream[String] = env.socketTextStream("localhost", 6666)
    val globalStream: DataStream[String] = inputStream1.global

    globalStream.print()
    env.executeAsync("A4_GlobalTest")
  }
}
