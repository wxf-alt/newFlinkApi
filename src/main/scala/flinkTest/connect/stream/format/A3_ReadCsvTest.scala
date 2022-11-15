package flinkTest.connect.stream.format

import java.io.File
import java.lang.reflect.Field
import java.util

import bean.CityPojo
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.PojoCsvInputFormat
import org.apache.flink.api.java.typeutils.{PojoField, PojoTypeInfo}
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.csv.CsvReaderFormat
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvSchema}
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/11/2 23:12:46
 * @Description: A3_ReadCsvTest
 * @Version 1.0.0
 */
object A3_ReadCsvTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)

    // 方式一：
    //    val somePojo: SomePojo = SomePojo("", "")
    //    val fields: Array[Field] = somePojo.getClass.getDeclaredFields
    //    val lst: util.ArrayList[PojoField] = new util.ArrayList[PojoField]()
    //    for (elem <- fields) {
    //      lst.add(new PojoField(elem, TypeInformation.of(elem.getType)))
    //    }
    //    val value: DataStream[SomePojo] = env.createInput(new PojoCsvInputFormat(new Path("E:\\A_data\\4.测试数据\\flink数据\\csv输出\\2022-11-02--23"), new PojoTypeInfo(classOf[SomePojo], lst)))
    //    value.print("value")


    // 方式二：
    val readFormat: CsvReaderFormat[CityPojo] = CsvReaderFormat.forPojo(classOf[CityPojo])
    val csvSource: FileSource[CityPojo] = FileSource.forRecordStreamFormat(readFormat, new Path("E:\\A_data\\4.测试数据\\flink数据\\csv输出\\2022-11-02--23")).build()
    val csvInputStream: DataStream[CityPojo] = env.fromSource(csvSource, WatermarkStrategy.noWatermarks(), "csv-source")
    csvInputStream.print("csvInputStream")


    // 方式三：
    //    val mapper: CsvMapper = new CsvMapper()
    //    // 定义 分隔符
    //    val schema: CsvSchema = mapper.schemaFor(classOf[CityPojo]).withoutQuoteChar().withColumnSeparator(',')
    //    val csvReadFormat: CsvReaderFormat[CityPojo] = CsvReaderFormat.forSchema(mapper, schema, TypeInformation.of(classOf[CityPojo]))
    //    val value: FileSource[CityPojo] = FileSource.forRecordStreamFormat(csvReadFormat, new Path("E:\\A_data\\4.测试数据\\flink数据\\csv输出\\2022-11-02--23")).build()
    //    val csvInputStream: DataStream[CityPojo] = env.fromSource(value, WatermarkStrategy.noWatermarks(), "csv-source")
    //    csvInputStream.print("csvInputStream")

    env.execute("A3_ReadCsvTest")
  }

  case class SomePojo(a: String, b: String)
}