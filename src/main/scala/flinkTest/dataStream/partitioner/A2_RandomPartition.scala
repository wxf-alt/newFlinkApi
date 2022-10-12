package flinkTest.dataStream.partitioner

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/10/10 19:16:31
 * @Description: A2_RandomPartition   随机分区 将元素随机地均匀划分到分区。
 * @Version 1.0.0
 */
object A2_RandomPartition {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(5)

    val inputStream1: DataStream[String] = env.socketTextStream("localhost", 6666)
    val shuffleStream: DataStream[String] = inputStream1.shuffle

    shuffleStream.print()
    env.executeAsync("A2_RandomPartition")
  }
}
