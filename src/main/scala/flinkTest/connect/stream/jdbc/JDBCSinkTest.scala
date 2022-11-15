package flinkTest.connect.stream.jdbc

import java.sql.PreparedStatement

import bean.Sensor
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2022/11/7 18:27:48
 * @Description: JDBCSinkTest
 * @Version 1.0.0
 */
object JDBCSinkTest {
  def main(args: Array[String]): Unit = {

    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    // 开启 checkPoint
    env.enableCheckpointing(1000)
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val mapStream: DataStream[Sensor] = inputStream.map(x => {
      val str: Array[String] = x.split(" ")
      Sensor(str(0), str(1) toLong, str(2).toDouble)
    })

    mapStream.print("mapStream：")

    // 幂等写入
    // Mysql --> insert into kid_score(id, birth_day, score) values (1,'2019-01-15',30) ON DUPLICATE KEY UPDATE score = score + 50
    // insert into sensor(sensor, ts, temperature) values ('sensor_3',165423078,32.5) ON DUPLICATE KEY UPDATE ts = 13524685,temperature=10.1
    val jdbcSink: SinkFunction[Sensor] = JdbcSink.sink(
      //      "insert into sensor (sensor, ts, temperature) values (?,?,?)",
      "insert into sensor(sensor, ts, temperature) values (?,?,?) ON DUPLICATE KEY UPDATE ts = ?,temperature = ?",
      new JdbcStatementBuilder[Sensor] {
        override def accept(ps: PreparedStatement, sensor: Sensor) = {
          ps.setString(1, sensor.id)
          ps.setLong(2, sensor.timeStamp)
          ps.setDouble(3, sensor.temperature)
          ps.setLong(4, sensor.timeStamp)
          ps.setDouble(5, sensor.temperature)
        }
      },
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withDriverName("com.mysql.jdbc.Driver")
        .withUrl("jdbc:mysql://localhost:3306/db?rewriteBatchedStatements=true&characterEncoding=utf-8")
        .withUsername("root")
        .withPassword("root")
        .build()
    )

    mapStream.addSink(jdbcSink)

    env.execute("JDBCSinkTest")
  }
}