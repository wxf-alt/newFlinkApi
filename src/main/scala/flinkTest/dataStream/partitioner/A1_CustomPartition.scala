package flinkTest.dataStream.partitioner

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/10/10 18:42:21
 * @Description: A1_CustomPartition 自定义分区
 * @Version 1.0.0
 */
object A1_CustomPartition {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(5)

    val inputStream1: DataStream[String] = env.socketTextStream("localhost", 6666)

    val partitionStream: DataStream[String] = inputStream1.partitionCustom(MyCustomPartition(5), x => x)
    partitionStream.print()

    env.executeAsync("A1_CustomPartition")
  }

  // 利用 Hash值 进行分区
  case class MyCustomPartition(numPartitions: Int) extends Partitioner[String] {
    override def partition(key: String, numPartitions: Int): Int = {
      val code: Int = key.hashCode
      code % numPartitions
    }
  }

}


