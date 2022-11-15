package flinkTest.connect.stream.format

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.{AvroParquetWriters}
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

import scala.util.Random

/**
 * @Auther: wxf
 * @Date: 2022/11/3 16:16:12
 * @Description: A5_WriteParquetFile
 * @Version 1.0.0
 */
object A5_WriteParquetFile {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    env.enableCheckpointing(5000)

    //    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val inputStream: DataStream[String] = env.addSource(new MySource())
    // 使用 Map 算子
    val mapStream: DataStream[ParquetTest] = inputStream
      .map(x => {
        val str: Array[String] = x.split(" ")
        ParquetTest(str(0), str(1).toInt * 2)
      })
    inputStream.print("inputStream：")

    val streamingFileSink: StreamingFileSink[ParquetTest] = StreamingFileSink.forBulkFormat(new Path("E:\\A_data\\4.测试数据\\flink数据\\Parquet输出"),
      AvroParquetWriters.forReflectRecord(classOf[ParquetTest])
    ).withBucketCheckInterval(10000)
      .withRollingPolicy(OnCheckpointRollingPolicy.build())
      .build()

    mapStream.addSink(streamingFileSink)

    env.execute("A5_WriteParquetFile")
  }

  case class ParquetTest(id: String, num: Int)

  class MySource extends RichParallelSourceFunction[String] {

    var flag: Boolean = true

    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
      val random: Random = new Random()
      while (flag) {
        val id: Int = random.nextInt(10) + 1
        val temp: Int = random.nextInt(100)
        Thread.sleep(id * 20)
        ctx.collect(s"sensor_${id} ${temp}")
      }
    }

    override def cancel(): Unit = flag = false

  }

}