package flinkTest.dataStream.source

import java.io.{File, FileInputStream}

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/10/17 20:03:57
 * @Description: A2_ParameterTest   应用程序参数处理
 * @Version 1.0.0
 */
object A2_ParameterTest {
  def main(args: Array[String]): Unit = {

    // 获取配置值
    // 1.配置值来自 .properties 文件
    val path: String = "/home/sam/flink/myjob.properties"
    val parameterTool1: ParameterTool = ParameterTool.fromPropertiesFile(path)
    val file: File = new File(path)
    val parameterTool2: ParameterTool = ParameterTool.fromPropertiesFile(file)
    val parameterTool3: ParameterTool = ParameterTool.fromPropertiesFile(new FileInputStream(file))

    // 2.配置值来自命令行
    val parameterTool4: ParameterTool = ParameterTool.fromArgs(args)

    // 3.配置值来自系统属性
    val parameterTool5: ParameterTool = ParameterTool.fromSystemProperties()

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(parameterTool1)

    val inputStream: DataStream[Int] = env.fromCollection(List(1, 2, 4, 5, 7, 8, 9, 6, 3, 5))
    val mapStream: DataStream[Int] = inputStream.map(new RichMapFunction[Int, Int] {
      override def map(value: Int) = {
        val parameters: ParameterTool = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
        val param: String = parameters.get("input")
        value
      }
    })

  }

}
