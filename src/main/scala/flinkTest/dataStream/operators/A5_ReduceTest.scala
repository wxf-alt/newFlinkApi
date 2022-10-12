package flinkTest.dataStream.t1_operators

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/10/9 16:15:01
 * @Description: A5_ReduceTest
 * @Version 1.0.0
 */
object A5_ReduceTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val keyedStream: KeyedStream[(String, Int), String] = inputStream
      .filter(filterFuntion(_))
      .map(x => {
        val str: Array[String] = x.split(" ")
        (str(0), str(1).toInt)
      })
      .keyBy(_._1)

    val resuceStream: DataStream[(String, Int)] = keyedStream.reduce((x, y) => (x._1, x._2 + y._2))

    resuceStream.print()

    env.execute("A5_ReduceTest")
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
