package flinkTest.dataStream.chaining

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/10/10 20:31:49
 * @Description: A4_SlotSharingGroupTest
 * @Version 1.0.0
 */
object A4_SlotSharingGroupTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)

    // 使用 Filter 算子
    //  将第二个字符串不为 int 的数据过滤掉
    val mapStream: DataStream[Int] = inputStream
      .filter(x => x.toInt > 1).slotSharingGroup("group1")  // 划分 slotGroup  将之后所有的算子都划分到设置的组下
      .map(_.toInt * 20)
      .map(_ % 5).slotSharingGroup("group2")

    mapStream.print()
    env.execute("A4_SlotSharingGroupTest")
  }
}
