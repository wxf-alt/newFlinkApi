package flinkTest.connect.stream.kafka


import java.time.Duration
import java.util.{HashMap, Properties}
//import java.lang.{Long => JLong}

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.serialization.TypeInformationKeyValueSerializationSchema
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.TopicPartition

/**
 * @Auther: wxf
 * @Date: 2022/11/4 14:56:03
 * @Description: KafkaSource
 * @Version 1.0.0
 */
object KafkaSourceTest {
  def main(args: Array[String]): Unit = {

    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    // 开启 checkPoint
    env.enableCheckpointing(1000)

    //    val executionConfig: ExecutionConfig = new ExecutionConfig()
    val topicPartitionMap: HashMap[TopicPartition, java.lang.Long] = new HashMap[TopicPartition, java.lang.Long]()
    topicPartitionMap.put(new TopicPartition("topic", 0), 10000L)

    val kafkaSource: KafkaSource[String] = KafkaSource.builder()
      .setBootstrapServers("bootStrapServers")
      .setGroupId("groupId")
      .setTopics("topic")
      // 从消费组提交的位点开始消费，如果提交位点不存在，使用最早位点
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
      // 指定停止消费位点
      .setUnbounded(OffsetsInitializer.offsets(topicPartitionMap))
      // 设置反序列化
      //      .setDeserializer(KafkaRecordDeserializationSchema.of(new TypeInformationKeyValueSerializationSchema(classOf[String], classOf[String], executionConfig)))
      .setValueOnlyDeserializer(new SimpleStringSchema())
      // 每 10 秒检查一次新分区
      .setProperty("partition.discovery.interval.ms", "10000")
      // 指定是否在进行 checkpoint 时将消费位点提交至 Kafka broker
      .setProperty("commit.offsets.on.checkpoint", "ture")
      .build()

    val kafkaInputStream: DataStream[String] = env.fromSource(kafkaSource,
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(500))
        // 处理空闲数据源
        .withIdleness(Duration.ofMillis(200))
        // 提取时间戳字段
        .withTimestampAssigner(new SerializableTimestampAssigner[String] {
          override def extractTimestamp(element: String, recordTimestamp: Long) = {
            val str: Array[String] = element.split(" ")
            str(1).toLong
          }
        }),
      "Kafka Source").uid("Source")

    kafkaInputStream.print("kafkaInputStream：").uid("sink")

    env.execute("KafkaSourceTest")
  }
}
