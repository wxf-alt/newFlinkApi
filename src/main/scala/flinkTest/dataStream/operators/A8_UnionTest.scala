package flinkTest.dataStream.t1_operators

import flinkTest.dataStream.t1_operators.A5_ReduceTest.filterFuntion
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Auther: wxf
 * @Date: 2022/10/9 16:59:02
 * @Description: A8_UnionTest
 *               将两个或多个数据流联合来创建一个包含所有流中数据的新流。
 *               注意：如果一个数据流和自身进行联合，这个流中的每个数据将在合并后的流中出现两次。
 * @Version 1.0.0
 */
object A8_UnionTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val inputStream1: DataStream[String] = env.socketTextStream("localhost", 6666)
    val inputStream2: DataStream[String] = env.socketTextStream("localhost", 7777)

    //    // 当前流 与 其他流 进行 union
    //    val inputStream: DataStream[String] = inputStream1.union(inputStream2)

    // 当前流 与 自己 进行 union   结果会出现两次
    val inputStream: DataStream[String] = inputStream1.union(inputStream1)

    val mapStream: DataStream[(String, Int)] = inputStream
      .filter(filterFuntion(_))
      .map(x => {
        val str: Array[String] = x.split(" ")
        (str(0), str(1).toInt)
      })

    mapStream.print()
    env.executeAsync("A8_UnionTest")
  }
}
