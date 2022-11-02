package flinkTest.dataStream.source

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/10/17 17:36:25
 * @Description: A1_MySource
 * @Version 1.0.0
 */
object A1_MySource {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val inputStream: DataStream[Int] = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9))
    //    val mapStream: DataStream[(Int, String)] = inputStream.map {
    //      case x: Int => {
    //        if (x > 5) (x, "大于5") else (x, "小于5")
    //      }
    //    }

    import org.apache.flink.streaming.api.scala.extensions._

    // mapWith
    val mapStream: DataStream[(Int, String)] = inputStream.mapWith {
      case x: Int => {
        if (x > 5) (x, "大于5") else (x, "小于5")
      }
    }

    val filterStream: DataStream[Int] = inputStream.filterWith({
      case x: Int => x > 5
    })

    mapStream.print("mapStream：")
    filterStream.print("filterStream：")
    env.executeAsync("A1_MySource")
  }

}
