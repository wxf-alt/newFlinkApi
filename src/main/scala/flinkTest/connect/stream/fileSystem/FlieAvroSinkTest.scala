package flinkTest.connect.stream.fileSystem

import java.io.OutputStream

import bean.Sensor
import flinkTest.connect.stream.format.A1_WriteAvroFormatTest.AvroTest
import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.io.{DatumWriter, Encoder}
import org.apache.avro.reflect.{ReflectData, ReflectDatumWriter}
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.avro.{AvroBuilder, AvroInputFormat, AvroWriterFactory}
import org.apache.flink.formats.parquet.avro.AvroParquetWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/11/7 20:06:56
 * @Description: FlieSourceeTest
 * @Version 1.0.0
 */
object FlieAvroSinkTest {
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
    val fileSink1: FileSink[Sensor] = FileSink.forBulkFormat[Sensor](new Path("E:\\A_data\\4.测试数据\\flink数据\\Avro输出"),
      new AvroWriterFactory(new AvroBuilder[Sensor] {
        override def createWriter(outputStream: OutputStream) = {
          val schema: Schema = ReflectData.get.getSchema(classOf[Sensor])
          val reflectDatumWriter: ReflectDatumWriter[Sensor] = new ReflectDatumWriter(schema)
          val dataFileWriter: DataFileWriter[Sensor] = new DataFileWriter(reflectDatumWriter)
          dataFileWriter.setCodec(CodecFactory.snappyCodec())
          dataFileWriter.create(schema, outputStream)
          dataFileWriter
        }
      })
    ).build()

    mapStream.sinkTo(fileSink1)

    // 验证输出结果文件
    //    val avroInput: AvroInputFormat[Sensor] = new AvroInputFormat(new Path("E:\\A_data\\4.测试数据\\flink数据\\Avro输出\\2022-11-08--10"), classOf[Sensor])
    //    val inputStream: DataStream[Sensor] = env.createInput(avroInput)
    //    inputStream.print("inputStream：")


    env.execute("FlieAvroSinkTest")
  }
}