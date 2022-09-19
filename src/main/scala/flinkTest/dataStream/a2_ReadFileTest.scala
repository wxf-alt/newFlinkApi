package flinkTest.dataStream

import org.apache.flink.api.common.io.FilePathFilter
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/9/13 11:40:22
 * @Description: a2_readFile  定期读取文件.可以使用过滤 只读取关闭的文件
 * @Version 1.0.0
 */
object a2_ReadFileTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8888)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val path: String = "E:\\A_data\\3.code\\newFlinkApi\\src\\main\\resources"
    val inputFormat: TextInputFormat = new TextInputFormat(new Path("E:\\A_data\\3.code\\newFlinkApi\\src\\main\\resources"))

    // 每1秒 扫描一次路径
    val inputStream: DataStream[String] = env.readFile(inputFormat, path, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000, FilePathFilter.createDefaultFilter())

    inputStream.print()

    env.executeAsync("a2_readFile")
  }
}
