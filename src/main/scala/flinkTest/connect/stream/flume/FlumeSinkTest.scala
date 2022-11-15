package flinkTest.connect.stream.flume

import flinkTest.connect.stream.format.A5_WriteParquetFile.MySource
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.flume.{FlumeEventBuilder, FlumeSink}
import org.apache.flume.event.SimpleEvent

/**
 * @Auther: wxf
 * @Date: 2022/11/7 17:53:13
 * @Description: FlumeSinkTest
 * @Version 1.0.0
 */
object FlumeSinkTest {
  def main(args: Array[String]): Unit = {

    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    // 开启 checkPoint
    env.enableCheckpointing(1000)
    val inputStream: DataStream[String] = env.addSource(new MySource)

    val flumeSink: FlumeSink[String] = new FlumeSink("", "", 3200, new FlumeEventBuilder[String]() {
      override def createFlumeEvent(in: String, runtimeContext: RuntimeContext) = {
        val event: SimpleEvent = new SimpleEvent()
        import scala.collection.convert.ImplicitConversions._
        event.setHeaders(Map("Sink-Flume" -> in))
        event.setBody(in.getBytes())
        event
      }
    })
    inputStream.addSink(flumeSink)

    env.execute("FlumeSinkTest")
  }
}