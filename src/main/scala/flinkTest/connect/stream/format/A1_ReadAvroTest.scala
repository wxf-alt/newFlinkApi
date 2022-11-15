package flinkTest.connect.stream.format

import flinkTest.connect.stream.format.A1_WriteAvroFormatTest.AvroTest
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.avro.AvroInputFormat
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/11/2 20:30:16
 * @Description: A2_ReadAvroTest
 * @Version 1.0.0
 */
object A1_ReadAvroTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val avroInput: AvroInputFormat[AvroTest] = new AvroInputFormat(new Path("E:\\A_data\\4.测试数据\\flink数据\\Avro输出\\2022-11-02--20"), classOf[AvroTest])
    val inputStream: DataStream[AvroTest] = env.createInput(avroInput)
    val mapStream: DataStream[(String, Int)] = inputStream.map(x => {
      (x.id, x.num / 2)
    })

    mapStream.print("mapStream")

    env.execute("A2_ReadAvroTest")
  }
}