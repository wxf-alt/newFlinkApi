package flinkTest.dataStream.chaining

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/10/10 20:24:27
 * @Description: A3_DisableChainingTest
 * @Version 1.0.0
 */
object A3_DisableChainingTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)

    // 使用 Filter 算子
    //  将第二个字符串不为 int 的数据过滤掉
    val mapStream: DataStream[Int] = inputStream
      .filter(x => x.toInt > 1)
      .map(_.toInt * 20)
      .disableChaining() // 断开链接
      .map(_ % 5)

    mapStream.print()
    env.execute("A3_DisableChainingTest")
  }
}
