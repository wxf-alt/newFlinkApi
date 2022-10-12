package flinkTest.dataStream.chaining

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/10/10 20:09:07
 * @Description: A1_DisableOperatorChainingTest  全局禁用 算子链
 * @Version 1.0.0
 */
object A1_DisableOperatorChainingTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.disableOperatorChaining()

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)

    // 使用 Filter 算子
    //  将第二个字符串不为 int 的数据过滤掉
    val filteStream: DataStream[String] = inputStream.filter(x => {
      val str: Array[String] = x.split(" ")
      var num: Any = str(1)
      try {
        num = str(1).toInt
      } catch {
        case exception: Exception => ""
      }
      str.length == 2 && num.isInstanceOf[Int]
    })

    filteStream.print()

    env.execute("A1_DisableOperatorChainingTest")
  }
}
