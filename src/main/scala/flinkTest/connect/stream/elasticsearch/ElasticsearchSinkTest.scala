package flinkTest.connect.stream.elasticsearch

import flinkTest.connect.stream.format.A5_WriteParquetFile.MySource
import org.apache.flink.api.connector.sink2.SinkWriter
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.elasticsearch.sink.{Elasticsearch7SinkBuilder, ElasticsearchEmitter, ElasticsearchSink, FlushBackoffType, RequestIndexer}
import org.apache.flink.streaming.api.scala._
import org.apache.http.HttpHost
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.Requests

/**
 * @Auther: wxf
 * @Date: 2022/11/7 14:50:01
 * @Description: ElasticsearchSinkTest
 * @Version 1.0.0
 */
object ElasticsearchSinkTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    // 开启 checkPoint
    env.enableCheckpointing(1000)

    val inputStream: DataStream[String] = env.addSource(new MySource)
    val elasticSink: ElasticsearchSink[String] = new Elasticsearch7SinkBuilder[String]
      // 下面的设置使 sink 在接收每个元素之后立即提交，否则这些元素将被缓存起来
      .setBulkFlushMaxActions(1)
      .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
      .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
      .setEmitter(new ElasticsearchEmitter[String]() {
        override def emit(element: String, context: SinkWriter.Context, indexer: RequestIndexer) = {
          val map: Map[String, String] = Map("value" -> element)
          // 使用具有确定性ID的UpdateRequests和upsert方法可以在Elasticsearch中实现一次语义
          //          val request: UpdateRequest = new UpdateRequest("my-index", "my-id")
          //          request.upsert(map)
          indexer.add(Requests.indexRequest.index("my-index").`type`("my-type").source(map))
        }
      })
      // 这里启用了一个指数退避重试策略，初始延迟为 1000 毫秒且最大重试次数为 5
      .setBulkFlushBackoffStrategy(FlushBackoffType.EXPONENTIAL, 5, 1000)
      .build()

    inputStream.sinkTo(elasticSink)

    env.execute("ElasticsearchSinkTest")
  }
}