package flinkTest.dataStream.t1_operators

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/10/9 14:50:42
 * @Description: A2_FlatMapTest
 * @Version 1.0.0
 */
object A2_FlatMapTest {
  def main(args: Array[String]): Unit = {

    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)

    // 使用 Map 算子
    val flatMapStream: DataStream[String] = inputStream.flatMap(x => x.split(" "))

    flatMapStream.print()

    env.execute("A2_FlatMapTest")
  }
}
