package flinkTest.connect.stream.elasticsearch

import java.util

import bean.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import scala.collection.mutable

/**
 * @Auther: wxf
 * @Date: 2022/6/14 20:26:41
 * @Description: ElasticsearchSink
 * @Version 1.0.0
 */
object EsSink {
  def main(args: Array[String]): Unit = {
    val path: String = this.getClass.getClassLoader.getResource("sensor.txt").getPath
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream: DataStream[String] = env.readTextFile(path)
    val transform: DataStream[SensorReading] = inputStream.map(s => {
      val str: Array[String] = s.split(" ")
      SensorReading(str(0), str(1).toLong, str(2).toDouble)
    })

    val hostsList: util.ArrayList[HttpHost] = new util.ArrayList[HttpHost]()
    hostsList.add(new HttpHost("nn1.hadoop", 9200))

    val esSinkFunc: ElasticsearchSinkFunction[SensorReading] = new ElasticsearchSinkFunction[SensorReading]() {
      override def process(element: SensorReading, ctx: RuntimeContext, indexer: RequestIndexer) = {
        // 包装写入es的数据
        val hashMap: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
        hashMap.put("sensor_id", element.id)
        hashMap.put("temp", element.temperature.toString)
        hashMap.put("ts", element.timestamp.toString)

        // 创建一个index request
        val indexRequest: IndexRequest = Requests.indexRequest()
          .index("sensor_temp")
          .`type`("readingdata")
          .source(hashMap)

        // 用indexer发送 http请求
        indexer.add(indexRequest)
        println(element + " saved successfully")
      }
    }
    transform.addSink(new ElasticsearchSink.Builder[SensorReading](hostsList, esSinkFunc).build())

    env.execute("ElasticsearchSink")
  }
}