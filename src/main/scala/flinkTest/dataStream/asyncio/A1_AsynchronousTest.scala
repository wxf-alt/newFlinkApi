package flinkTest.dataStream.asyncio

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}

import scala.util.Random


/**
 * @Auther: wxf
 * @Date: 2022/10/14 15:38:33
 * @Description: A6_AsynchronousTest1
 * @Version 1.0.0
 */
object A1_AsynchronousTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val mapStream: DataStream[Int] = inputStream.map(x => x.toInt).setParallelism(6)

    val asynStream: DataStream[(Int, String, String)] = AsyncDataStream.unorderedWait(mapStream, new AsyncDemoUser, 3, TimeUnit.SECONDS, 100).setParallelism(6)

    asynStream.print()
    env.execute("Async IO Demo")
  }

  class AsyncDemoUser extends RichAsyncFunction[Int, (Int, String, String)] {

    //定义驱动,数据库地址,名称,密码
    private val driver: String = "com.mysql.jdbc.Driver"

    private val url: String = "jdbc:mysql://localhost:3306/db" //ip为数据库所在ip，若为本机数据库则是localhost；databasename为要链接的数据库名
    private val userName: String = "root" //连接所用用户名
    private val password: String = "root" //连接所用密码
    //获取连接
    private var connection: Connection = null
    var random: Random = null
    var date: Date = null
    var simpleDateFormat: SimpleDateFormat = null

    // 模拟 异步 延迟
    private val sleep: Array[Long] = Array(100L, 1000L, 5000L, 2000L, 6000L, 100L)

    override def open(parameters: Configuration): Unit = {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, userName, password)
      random = new Random()
      date = new Date()
      simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")
    }

    // 为每个流输入触发异步操作
    override def asyncInvoke(input: Int, resultFuture: ResultFuture[(Int, String, String)]): Unit = {
      val l: Long = System.currentTimeMillis()
      Thread.sleep(sleep(random.nextInt(6)))
      val statement: PreparedStatement = connection.prepareStatement("select s_id, s_name from student1 where s_id = ?")
      statement.setInt(1, input)
      val resultSet: ResultSet = statement.executeQuery()
      while (resultSet.next()) {
        val name: String = resultSet.getString(2)
        date.setTime(l)
        val currentTimeStamp: String = simpleDateFormat.format(date)
        resultFuture.complete(List((input, name, currentTimeStamp)))

      }
    }


    override def timeout(input: Int, resultFuture: ResultFuture[(Int, String, String)]): Unit = {
      resultFuture.complete(Nil)
    }

    override def close(): Unit = {
      connection.close()
    }

  }

}