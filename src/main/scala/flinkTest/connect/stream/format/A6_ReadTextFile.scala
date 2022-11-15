package flinkTest.connect.stream.format

import java.time.Duration
import java.util.function.Predicate

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.connector.file.src.compression.StandardDeCompressors
import org.apache.flink.connector.file.src.enumerate.FileEnumerator.Provider
import org.apache.flink.connector.file.src.enumerate.{BlockSplittingRecursiveEnumerator, DefaultFileFilter, FileEnumerator}
import org.apache.flink.connector.file.src.reader.TextLineInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/11/4 14:34:51
 * @Description: A6_ReadTextFile
 * @Version 1.0.0
 */
object A6_ReadTextFile {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8082)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    val fileSource: FileSource[String] = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("E:\\A_data\\4.测试数据\\flink数据\\Text输出\\2022-11-04--16"))
      .monitorContinuously(Duration.ofSeconds(1L))
//      .setFileEnumerator(new Provider {
//        override def create() = {
//          new BlockSplittingRecursiveEnumerator(new Predicate[Path] {
//            override def test(path: Path): Boolean = {
//              val fileName: String = path.getName
//              println("fileName:" + fileName)
//              if (fileName == null || fileName.length == 0) true
//              else if (fileName.endsWith(".txt")) true
//              else false
//            }
//          }, StandardDeCompressors.getCommonSuffixes.toArray(new Array[String](0)))
//        }
//      })
      .build()
    val inputStream: DataStream[String] = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source")
    inputStream.print("inputStream：")
    env.execute("A6_ReadTextFile")
  }
}