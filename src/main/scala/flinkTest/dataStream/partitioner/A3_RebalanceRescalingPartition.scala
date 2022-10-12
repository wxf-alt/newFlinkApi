package flinkTest.dataStream.partitioner

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @Auther: wxf
 * @Date: 2022/10/10 19:19:01
 * @Description: A3_RescalingPartition   相比与 rebalance(完全轮询发送)；rescaling(组内轮询发送)
 * @Version 1.0.0
 */
object A3_RebalanceRescalingPartition {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(5)

    val inputStream1: DataStream[String] = env.socketTextStream("localhost", 6666)
    val rescaleStream: DataStream[String] = inputStream1.rescale

    val rebalanceStream: DataStream[String] = inputStream1.rebalance

    rescaleStream.print()
    rebalanceStream.print()
    env.executeAsync("A3_RescalingPartition")
  }
}
