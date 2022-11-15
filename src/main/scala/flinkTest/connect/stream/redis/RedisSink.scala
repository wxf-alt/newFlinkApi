package flinkTest.connect.stream.redis

import bean.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @Auther: wxf
 * @Date: 2022/6/14 20:26:13
 * @Description: RedisSink
 * @Version 1.0.0
 */
object RedisSink {
  def main(args: Array[String]): Unit = {

    val path: String = this.getClass.getClassLoader.getResource("sensor.txt").getPath
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream: DataStream[String] = env.readTextFile(path)
    val trancFrom: DataStream[SensorReading] = inputStream.map(s => {
      val str: Array[String] = s.split(" ")
      SensorReading(str(0), str(1).toLong, str(2).toDouble)
    })

    val jedisPoolConfig: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("nn1.hadoop")
      .setPort(6379)
      .build()
    val redisMapper: RedisMapper[SensorReading] = new RedisMapper[SensorReading]() {
      // 设置写入 Redis 的命令，和 Key
      override def getCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")
      }
      override def getKeyFromData(data: SensorReading) = data.id
      override def getValueFromData(data: SensorReading) = data.temperature.toString
    }

    trancFrom.addSink(new RedisSink(jedisPoolConfig,redisMapper))

    env.execute("RedisSink")
  }
}