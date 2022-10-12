package flinkTest.dataStream.chaining

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/10/10 20:15:16
 * @Description: A2_StartNewChainTest
 * @Version 1.0.0
 */
object A2_StartNewChainTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)

    // 使用 Filter 算子
    //  将第二个字符串不为 int 的数据过滤掉
    val mapStream: DataStream[(Int, Int)] = inputStream
      .filter(x => x.toInt > 1)
      .map(x => x.toInt * 20)
      .startNewChain() // 创建 新链接(作用于 调用当前算子的转换算子操作)
      .map(x => (x, x % 5))

    mapStream.print()

    env.execute("A2_StartNewChainTest")
  }
}
