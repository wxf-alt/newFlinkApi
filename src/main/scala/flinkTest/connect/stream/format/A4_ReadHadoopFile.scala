package flinkTest.connect.stream.format

import org.apache.flink.api.scala.hadoop.mapreduce.HadoopInputFormat
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.hadoopcompatibility.scala.HadoopInputs
import org.apache.flink.streaming.api.scala._
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

/**
 * @Auther: wxf
 * @Date: 2022/11/3 11:33:12
 * @Description: A4_ReadHadoopFile
 * @Version 1.0.0
 */
object A4_ReadHadoopFile {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val hadoopInputFormat: HadoopInputFormat[LongWritable, Text] = HadoopInputs.readHadoopFile(new TextInputFormat(), classOf[LongWritable], classOf[Text], "E:\\A_data\\4.测试数据\\flink数据\\Hadoop输入")
    val inputStream: DataStream[(LongWritable, Text)] = env.createInput(hadoopInputFormat)
    inputStream.print("inputStream")

    env.execute("A4_ReadHadoopFile")
  }
}