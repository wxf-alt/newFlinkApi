package flinkTest.connect.stream.kafka

import flinkTest.connect.stream.format.A5_WriteParquetFile.MySource
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink, KafkaSinkBuilder}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.kafka.common.serialization.StringSerializer

/**
 * @Auther: wxf
 * @Date: 2022/11/5 18:03:23
 * @Description: KafkaSinkTest
 * @Version 1.0.0
 */
object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    // 开启 checkPoint
    env.enableCheckpointing(1000)

    //    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val inputStream: DataStream[String] = env.addSource(new MySource)
    val kafkaSink: KafkaSink[String] = KafkaSink.builder()
      // 设置 broke 机器
      .setBootstrapServers("nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092")
      // 设置序列化
      .setRecordSerializer(
        KafkaRecordSerializationSchema.builder()
          .setTopic("topic")
          .setKeySerializationSchema(new SimpleStringSchema())
          .setValueSerializationSchema(new SimpleStringSchema())
          .build()
      )
      // 设置 一致性语义
      .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
      // 语义是 EXACTLY_ONCE 需要设置 事务 ID
      .setTransactionalIdPrefix("KafkaSinkTest-Transaction")
      .build()

    inputStream.sinkTo(kafkaSink)

    env.execute("KafkaSinkTest")
  }
}