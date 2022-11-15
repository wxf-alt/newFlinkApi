package flinkTest.connect.stream.redis

import java.net.InetSocketAddress

import flinkTest.connect.stream.format.A5_WriteParquetFile.MySource
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisClusterConfig, FlinkJedisPoolConfig, FlinkJedisSentinelConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

import scala.collection.mutable

/**
 * @Auther: wxf
 * @Date: 2022/11/7 15:51:45
 * @Description: RedisSinkTest
 * @Version 1.0.0
 */
object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    // 开启 checkPoint
    env.enableCheckpointing(1000)

    val inputStream: DataStream[String] = env.addSource(new MySource)

    // 单节点 Redis
    val jedisPoolConfig: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build()
    val jedisPoolSink: RedisSink[String] = new RedisSink(jedisPoolConfig, RedisExampleMapper())

    // 集群 Redis
    val addresses: mutable.Set[InetSocketAddress] = mutable.Set[InetSocketAddress]()
    addresses += (new InetSocketAddress("nn1.hadoop", 6379))
    addresses += (new InetSocketAddress("nn2.hadoop", 6379))
    addresses += (new InetSocketAddress("s1.hadoop", 6379))
    import scala.collection.convert.ImplicitConversions._
    val jedisClusterConfig: FlinkJedisClusterConfig = new FlinkJedisClusterConfig.Builder().setNodes(addresses).build()
    val jedisClusterSink: RedisSink[String] = new RedisSink(jedisClusterConfig, RedisExampleMapper())

    // 哨兵 Redis
    val jedisSentinelConfig: FlinkJedisSentinelConfig = new FlinkJedisSentinelConfig.Builder().setMasterName("master").setSentinels(Set("")).build()
    val jedisSentinelSink: RedisSink[String] = new RedisSink(jedisSentinelConfig, RedisExampleMapper())

    inputStream.addSink(jedisClusterSink)

    env.execute("RedisSinkTest")
  }

  case class RedisExampleMapper() extends RedisMapper[String] {
    override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET, "HASH_NAME")

    override def getKeyFromData(data: String): String = data.substring(0, 3)

    override def getValueFromData(data: String): String = data.substring(3)
  }

}