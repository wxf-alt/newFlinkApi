package flinkTest.dataStream.asyncio

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import flinkTest.dataStream.asyncio.A1_AsynchronousTest.AsyncDemoUser
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

import scala.util.Random

/**
 * @Auther: wxf
 * @Date: 2022/10/17 11:36:42
 * @Description: A2_SynchronousTest  创建 同步流 对比 异步IO
 * @Version 1.0.0
 */
object A2_SynchronousTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val mapStream: DataStream[Int] = inputStream.map(x => x.toInt).setParallelism(4)

    val syncStream: DataStream[(Int, String, String)] = mapStream.map(new RichMapFunction[Int, (Int, String, String)] {

      //定义驱动,数据库地址,名称,密码
      private val driver: String = "com.mysql.jdbc.Driver"
      //获取连接
      private var connection: Connection = null

      private val url: String = "jdbc:mysql://localhost:3306/db" //ip为数据库所在ip，若为本机数据库则是localhost；databasename为要链接的数据库名
      private val userName: String = "root" //连接所用用户名
      private val password: String = "root" //连接所用密码

      // 模拟 异步 延迟
      private val sleep: Array[Long] = Array(100L, 1000L, 5000L, 2000L, 6000L, 100L)
      var random: Random = null
      var date: Date = null
      var simpleDateFormat: SimpleDateFormat = null

      override def open(parameters: Configuration): Unit = {
        println(getRuntimeContext.getIndexOfThisSubtask + "创建 MySql 连接")
        Class.forName(driver)
        connection = DriverManager.getConnection(url, userName, password)
        random = new Random()
        date = new Date()
        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")
      }

      override def map(value: Int): (Int, String, String) = {
        val l: Long = System.currentTimeMillis()
        Thread.sleep(sleep(random.nextInt(6)))
        val statement: PreparedStatement = connection.prepareStatement("select s_id, s_name from student1 where s_id = ?")
        statement.setInt(1, value)
        val resultSet: ResultSet = statement.executeQuery()
        var name: String = ""
        var currentTimeStamp: String = ""
        while (resultSet.next()) {
          name = resultSet.getString(2)
          date.setTime(l)
          currentTimeStamp = simpleDateFormat.format(date)
        }
        (value, name, currentTimeStamp)
      }

      override def close(): Unit = {
        connection.close()
      }

    }).setParallelism(8)


    syncStream.print()
    env.execute("Async IO Demo")
  }
}