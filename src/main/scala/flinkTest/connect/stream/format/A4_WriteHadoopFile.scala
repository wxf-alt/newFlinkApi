package flinkTest.connect.stream.format

import org.apache.flink.api.scala.hadoop.mapred.HadoopOutputFormat
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf, TextOutputFormat}

/**
 * @Auther: wxf
 * @Date: 2022/11/3 11:57:00
 * @Description: A4_WriteHadoopFile
 * @Version 1.0.0
 */
object A4_WriteHadoopFile {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    //    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val inputStream: DataStream[String] = env.readTextFile("E:\\A_data\\4.测试数据\\flink数据\\Hadoop输入")

    // 使用 Map 算子
    val mapStream = inputStream
      .map(x => {
        val str: Array[String] = x.split(" ")
        val text: Text = new Text(str(0))
        val intWritable: IntWritable = new IntWritable(str(1).toInt)
        (text, intWritable)
      })
    mapStream.print("mapStream：")

    val hadoopOF: HadoopOutputFormat[Text, IntWritable] = new HadoopOutputFormat[Text, IntWritable](
      new TextOutputFormat[Text, IntWritable],
      new JobConf)

    hadoopOF.getJobConf.set("mapred.textoutputformat.separator", " ")
    FileOutputFormat.setOutputPath(hadoopOF.getJobConf, new Path("E:\\A_data\\4.测试数据\\flink数据\\Hadoop输出"))

    mapStream.writeUsingOutputFormat(hadoopOF)

    env.execute("A4_WriteHadoopFile")
  }
}