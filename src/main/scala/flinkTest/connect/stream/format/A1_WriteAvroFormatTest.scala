package flinkTest.connect.stream.format

import java.io.OutputStream
import java.time.Duration
import java.util.concurrent.TimeUnit

import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.reflect.{ReflectData, ReflectDatumWriter}
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.avro.{AvroBuilder, AvroWriterFactory, AvroWriters}
import org.apache.flink.streaming.api.functions.sink.filesystem.{OutputFileConfig, PartFileInfo}
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.{CheckpointRollingPolicy, DefaultRollingPolicy, OnCheckpointRollingPolicy}
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/11/2 19:40:43
 * @Description: A1_AvroFormatTest
 * @Version 1.0.0
 */
object A1_WriteAvroFormatTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    env.enableCheckpointing(1000)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)

    // 使用 Map 算子
    val mapStream: DataStream[AvroTest] = inputStream
      .map(x => {
        val str: Array[String] = x.split(" ")
        AvroTest(str(0), str(1).toInt * 2)
      })

    val factory: AvroWriterFactory[AvroTest] = new AvroWriterFactory[AvroTest](new AvroBuilder[AvroTest] {
      override def createWriter(outputStream: OutputStream) = {
        val schema = ReflectData.get.getSchema(classOf[AvroTest])
        val datumWriter = new ReflectDatumWriter[AvroTest](schema)

        val dataFileWriter = new DataFileWriter[AvroTest](datumWriter)
        dataFileWriter.setCodec(CodecFactory.snappyCodec)
        dataFileWriter.create(schema, outputStream)
        dataFileWriter
      }
    })

    val sink: FileSink[AvroTest] = FileSink.forBulkFormat(new Path("E:\\A_data\\4.测试数据\\flink数据\\Avro输出"), factory)
      .withRollingPolicy(OnCheckpointRollingPolicy.build())
      .build()

    mapStream.sinkTo(sink)

    env.execute("A1_WriteAvroFormatTest")
  }

  case class AvroTest(id: String, num: Int)

}
