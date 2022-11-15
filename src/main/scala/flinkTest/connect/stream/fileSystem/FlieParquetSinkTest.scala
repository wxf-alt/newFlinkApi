package flinkTest.connect.stream.fileSystem

import bean.Sensor
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.connector.file.src.impl.StreamFormatAdapter
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.AvroParquetWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/11/7 20:06:56
 * @Description: FlieSourceeTest
 * @Version 1.0.0
 */
object FlieParquetSinkTest {
  def main(args: Array[String]): Unit = {

    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    // 开启 checkPoint
    env.enableCheckpointing(2000)
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val mapStream: DataStream[Sensor] = inputStream.map(x => {
      val str: Array[String] = x.split(" ")
      Sensor(str(0), str(1) toLong, str(2).toDouble)
    })

    // 类似这样使用 FileSink 写入 Parquet Format 的 Avro 数据
    val fileSink1: FileSink[Sensor] = FileSink.forBulkFormat[Sensor](new Path("E:\\A_data\\4.测试数据\\flink数据\\Parquet输出\\ParquetProtoWriters"),
      AvroParquetWriters.forReflectRecord(classOf[Sensor])
    )
      .withBucketCheckInterval(1000)
      .withRollingPolicy(OnCheckpointRollingPolicy.build())
      .build()

    mapStream.sinkTo(fileSink1)

    env.execute("FlieParquetSinkTest")
  }
}