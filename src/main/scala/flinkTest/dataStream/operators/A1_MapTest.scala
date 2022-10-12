package flinkTest.dataStream.t1_operators

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/10/9 14:47:42
 * @Description: A1_MapTest
 * @Version 1.0.0
 */
object A1_MapTest {
  def main(args: Array[String]): Unit = {

    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)

    // 使用 Map 算子
    val mapStream: DataStream[String] = inputStream
      .map(x => {
        val str: Array[String] = x.split(" ")
        (str(0) + "--" + str(1).toInt * 2)
      })

    mapStream.print()

    env.execute("A1_MapTest")
  }
}
