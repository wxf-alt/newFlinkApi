package flinkTest.dataStream.t1_operators

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/10/9 15:56:22
 * @Description: A4_KeyByTest  根据 元组 第一个字段 分区
 * @Version 1.0.0
 */
object A4_KeyByTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val keyedStream: KeyedStream[(String, String), String] = inputStream
      .filter(filterFuntion(_))
      .map(x => {
        val str: Array[String] = x.split(" ")
        (str(0), str(1))
      })
      .keyBy(_._1)

    keyedStream.print()

    env.execute("A4_KeyByTest")
  }

  val filterFuntion: String => Boolean = (x: String) => {
    val str: Array[String] = x.split(" ")
    var num: Any = str(1)
    try {
      num = str(1).toInt
    } catch {
      case exception: Exception => ""
    }
    str.length == 2 && num.isInstanceOf[Int]
  }

}
