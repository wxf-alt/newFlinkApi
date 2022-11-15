package flinkTest.connect.stream.fileSystem

import java.io.{DataInput, DataOutput}

import bean.Sensor
import org.apache.flink.api.java.tuple
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.connector.file.sink.compactor.{ConcatFileCompactor, DecoderBasedReader, FileCompactStrategy, RecordWiseFileCompactor, SimpleStringDecoder}
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.sequencefile.SequenceFileWriterFactory
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, OutputFileConfig}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.{DateTimeBucketAssigner, SimpleVersionedStringSerializer}
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import org.apache.flink.streaming.api.scala._
import org.apache.hadoop.conf
import org.apache.hadoop.io.{LongWritable, Text, Writable}

/**
 * @Auther: wxf
 * @Date: 2022/11/8 14:11:23
 * @Description: SequenceFileSinkText
 * @Version 1.0.0
 */
object SequenceFileSinkText {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    // 开启 checkPoint
    env.enableCheckpointing(5000)
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val mapStream: DataStream[tuple.Tuple2[Text, SensorWritable]] = inputStream.map(x => {
      val str: Array[String] = x.split(" ")
      val value: tuple.Tuple2[Text, SensorWritable] = new tuple.Tuple2[Text, SensorWritable]()
      value.f0 = new Text(str(0))
      value.f1 = SensorWritable(str(0), str(1) toLong, str(2).toDouble)
      value
    })
    mapStream.print("mapStream：")

    val outputFileConfig: OutputFileConfig = OutputFileConfig
      .builder()
      .withPartPrefix("prefix")
      .withPartSuffix(".ext")
      .build()

    val hadoopConf: org.apache.hadoop.conf.Configuration = new org.apache.hadoop.conf.Configuration()
    val value: FileSink[tuple.Tuple2[Text, SensorWritable]] = FileSink.forBulkFormat(new Path("E:\\A_data\\4.测试数据\\flink数据\\SequenceFile输出3"),
      new SequenceFileWriterFactory(hadoopConf, classOf[Text], classOf[SensorWritable]))
      // 设置 子目录 格式
      .withBucketAssigner(new DateTimeBucketAssigner[tuple.Tuple2[Text, SensorWritable]]("yyyy-MM-dd"))
      //      // 按照 key 进行 分桶
      //      .withBucketAssigner(new BucketAssigner[tuple.Tuple2[Text, SensorWritable], String]() {
      //        override def getBucketId(element: tuple.Tuple2[Text, SensorWritable], context: BucketAssigner.Context) = element.f1.id
      //
      //        override def getSerializer: SimpleVersionedStringSerializer = SimpleVersionedStringSerializer.INSTANCE
      //      })
      // 设置 文件 前缀,后缀
      .withOutputFileConfig(outputFileConfig)
      // 在每个检查点上滚动(ONLY)
      .withRollingPolicy(OnCheckpointRollingPolicy.build())
      // 设置 文件合并
      .enableCompact(FileCompactStrategy.Builder.newBuilder()
        .setNumCompactThreads(1024)
        .setSizeThreshold(1024)
        .enableCompactionOnCheckpoint(10)
        .build(),
        new ConcatFileCompactor()
      )
      .build()

    mapStream.sinkTo(value).uid("sink")

    env.execute("SequenceFileSinkText")
  }

  case class SensorWritable(id: String, timeStamp: Long, temperature: Double) extends Writable {
    override def write(dataOutput: DataOutput): Unit = {
      dataOutput.writeUTF(id)
      dataOutput.writeLong(timeStamp)
      dataOutput.writeDouble(temperature)
    }

    override def readFields(dataInput: DataInput): Unit = {
      dataInput.readUTF()
      dataInput.readLong()
      dataInput.readDouble()
    }

  }

}