package flinkTest.dataStream.operators

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Auther: wxf
 * @Date: 2022/10/10 17:37:58
 * @Description: A13_IterateTest
 *               业务场景：基于 Key 的窗口求和，如果窗口结果不满足条件，就重新进入窗口，再求和
 * @Version 1.0.0
 */
object A13_IterateTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val inputStream1: DataStream[String] = env.socketTextStream("localhost", 6666)

    //    val iterateStream: DataStream[Int] = inputStream1.iterate(iteration => {
    //      val iterationBody: DataStream[Int] = iteration.map { x => x.toInt }
    //      (iterationBody.filter(_ > 10).map(_.toString), iterationBody.filter(_ <= 10))
    //    })

    val mapStream: DataStream[(String, Int)] = inputStream1.map(x => {
      val str: Array[String] = x.split(" ")
      (str(0), str(1).toInt)
    }).disableChaining

    val iterateStream: DataStream[(String, Int)] = mapStream.iterate(iteration => {
      // 窗口求和
      val sumStream: DataStream[(String, Int)] = iteration.keyBy(_._1)
        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
        .reduce((x, y) => (x._1, x._2 + y._2))
      // 反馈分支：窗口输出数据小于 500，反馈到 mapStream，重新窗口求和
      (sumStream.filter(_._2 <= 500), sumStream.filter(_._2 > 500))
    }).disableChaining()

    iterateStream.print()
    env.executeAsync("A13_IterateTest")
  }
}