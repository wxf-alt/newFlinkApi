package flinkTest.dataStream.chaining

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/10/10 20:45:36
 * @Description: A5_NameDescription
 * @Version 1.0.0
 */
object A5_NameDescription {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.disableOperatorChaining()

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)

    // 使用 Filter 算子
    //  将第二个字符串不为 int 的数据过滤掉
    val mapStream: DataStream[Int] = inputStream.name("socket").setDescription("socket input stream")
      .filter(x => x.toInt > 1).name("filter").setDescription("filter num > 1")
      .map(_.toInt * 20).name("map1").setDescription("transformation num product 20")
      .map(_ % 5).name("map2").setDescription("transformation num divide 5")

    mapStream.print()
    env.execute("A5_NameDescription")
  }
}
