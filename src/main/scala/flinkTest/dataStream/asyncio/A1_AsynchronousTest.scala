package flinkTest.dataStream.asyncio

import java.math.BigInteger
import java.security.MessageDigest
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import com.alibaba.fastjson.JSONObject
import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.util.ExecutorUtils

import scala.util.Random


/**
 * @Auther: wxf
 * @Date: 2022/10/14 15:38:33
 * @Description: A6_AsynchronousTest1
 * @Version 1.0.0
 */
object A1_AsynchronousTest {
  def main(args: Array[String]): Unit = {
    //    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.setParallelism(2)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val mapStream: DataStream[Int] = inputStream.map(x => x.toInt).setParallelism(6)

    val asynStream: DataStream[(Int, String, String)] = AsyncDataStream.orderedWait(mapStream, new AsyncDemoUser1, 5, TimeUnit.SECONDS, 10)

    asynStream.print()
    env.execute("Async IO Demo")
  }

  // 同步 ID - 需要使用 异步客户端|线程池方式 连接数据库 查询数据
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
        println(Thread.currentThread() + "----------" + (input, name, currentTimeStamp).toString())
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

  // 线程池 创建客户端 连接
  class AsyncDemoUser1 extends RichAsyncFunction[Int, (Int, String, String)] {

    //定义驱动,数据库地址,名称,密码
    private val driver: String = "com.mysql.jdbc.Driver"

    private val url: String = "jdbc:mysql://localhost:3306/db" //ip为数据库所在ip，若为本机数据库则是localhost；databasename为要链接的数据库名
    private val userName: String = "root" //连接所用用户名
    private val password: String = "root" //连接所用密码

    //获取连接
    private var connection: Connection = _
    var random: Random = _
    var date: Date = _
    var simpleDateFormat: SimpleDateFormat = _

    var executorService: ExecutorService = _
    var cache: Cache[String, String] = _

    //使用opentsdb 的 异步客户端  http://opentsdb.github.io/asynchbase/javadoc/index.html
    override def open(parameters: Configuration): Unit = {
      //      connection = DriverManager.getConnection(url, userName, password)
      random = new Random()
      date = new Date()
      simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")
      //初始化线程池 12个线程
      executorService = Executors.newFixedThreadPool(12)
      //初始化缓存
      cache = CacheBuilder.newBuilder()
        .concurrencyLevel(12) //设置并发级别 允许12个线程同时访问
        .expireAfterAccess(2, TimeUnit.HOURS) //设置缓存 2小时 过期
        .maximumSize(10000) //设置缓存大小
        .build()
    }

    def timeout(input: String, resultFuture: ResultFuture[String]): Unit = {
      resultFuture.complete(Array("timeout:" + input))
    }

    //    // 模拟 异步 延迟
    //    private val sleep: Array[Long] = Array(100L, 1000L, 5000L, 2000L, 6000L, 100L)

    // 为每个流输入触发异步操作
    override def asyncInvoke(input: Int, resultFuture: ResultFuture[(Int, String, String)]): Unit = {
      executorService.submit(new Runnable {
        override def run(): Unit = {
          try {
            // 创建连接
            Class.forName(driver)
            connection = DriverManager.getConnection(url, userName, password)
            val l: Long = System.currentTimeMillis()
            //            Thread.sleep(sleep(random.nextInt(6)))
            val statement: PreparedStatement = connection.prepareStatement("select s_id, s_name from student1 where s_id = ?")
            statement.setInt(1, input)
            val resultSet: ResultSet = statement.executeQuery()
            while (resultSet.next()) {
              val name: String = resultSet.getString(2)
              date.setTime(l)
              val currentTimeStamp: String = simpleDateFormat.format(date)
              println(Thread.currentThread().getName + "----------" + (input, name, currentTimeStamp).toString())
              resultFuture.complete(List((input, name, currentTimeStamp)))
            }
          } catch {
            case e: Exception => resultFuture.complete(List((1, "error:" + e.printStackTrace(), "")))
          }
        }
      })
    }

    override def timeout(input: Int, resultFuture: ResultFuture[(Int, String, String)]): Unit = {
      resultFuture.complete(Nil)
    }

    override def close(): Unit = {
      ExecutorUtils.gracefulShutdown(100, TimeUnit.MILLISECONDS, executorService);
    }

  }

}