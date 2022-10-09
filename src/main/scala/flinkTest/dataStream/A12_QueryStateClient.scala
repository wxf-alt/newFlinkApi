package flinkTest.dataStream

import java.util.concurrent.CompletableFuture

import org.apache.flink.api.common.JobID
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.queryablestate.client.QueryableStateClient
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}

/**
 * @Auther: wxf
 * @Date: 2022/9/16 17:29:40
 * @Description: a12_QueryStateTest  读取 Flink 保存的状态
 * @Version 1.0.0
 */
object A12_QueryStateClient {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val keyStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val mapStream: DataStream[String] = keyStream
      .map((_, 1))
      .keyBy(_._1)
      .map(new RichMapFunction[(String, Int), String] {

        var stateClient: QueryableStateClient = _
        var average: ValueStateDescriptor[(Long, Double)] = _

        override def open(parameters: Configuration) = {
          import org.apache.flink.queryablestate.client.QueryableStateClient
          stateClient = new QueryableStateClient("s1.hadoop", 9069)
          average = new ValueStateDescriptor[(Long, Double)]("average", createTypeInformation[(Long, Double)])
        }

        override def map(value: (String, Int)): String = {
          val key: String = value._1
          val kvState: CompletableFuture[ValueState[(Long, Double)]] = stateClient.getKvState(
            JobID.fromHexString("1f15cc7285470682b2f3d358562b60ea"),
            "query-name", key, BasicTypeInfo.STRING_TYPE_INFO, average)
          try {
            val (valueState, valueState2): (Long, Double) = kvState.get().value()
            s"${key}：(${valueState},${valueState2})"
          } catch {
            case e: Exception => e.getMessage
          }
        }

      })

    mapStream.print("mapStream：")

    env.execute("Test1")

    //    import org.apache.flink.queryablestate.client.QueryableStateClient
    //    val stateClient: QueryableStateClient = new QueryableStateClient("s1.hadoop", 9069)
    //    val average: ValueStateDescriptor[(Long, Double)] = new ValueStateDescriptor[(Long, Double)]("average", createTypeInformation[(Long, Double)])
    //
    //    while (true) {
    //      val key: String = StdIn.readLine()
    //      val value: CompletableFuture[ValueState[(Long, Double)]] = stateClient.getKvState(
    //        JobID.fromHexString("1f15cc7285470682b2f3d358562b60ea"),
    //        "query-name", key, BasicTypeInfo.STRING_TYPE_INFO, average)
    //      try {
    //        val result: (Long, Double) = value.get().value()
    //        println(s"${key}：${result}")
    //      } catch {
    //        case e: Exception => println(e.getMessage)
    //      }
    //    }

  }

}