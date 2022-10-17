package flinkTest

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

/**
 * @Auther: wxf
 * @Date: 2022/10/17 10:08:14
 * @Description: ObjectMySql
 * @Version 1.0.0
 */
object ObjectMySql {

  //定义驱动,数据库地址,名称,密码
  private val driver: String = "com.mysql.jdbc.Driver"

  private val url: String = "jdbc:mysql://localhost:3306/db" //ip为数据库所在ip，若为本机数据库则是localhost；databasename为要链接的数据库名
  private val userName: String = "root" //连接所用用户名
  private val password: String = "root" //连接所用密码
  //获取连接
  private var connection: Connection = null

  def getConnection(): Unit = {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, userName, password)
  }

  def main(args: Array[String]): Unit = {
    getConnection()
    //    val statement: PreparedStatement = connection.prepareStatement("select s_id, s_name from student1")
    //    val resultSet: ResultSet = statement.executeQuery()
    //    while(resultSet.next()){
    //      val id: String = resultSet.getString(1)
    //      val name: String = resultSet.getString(2)
    //      println(s"id：${id},name：${name}")
    //    }

    val statement: PreparedStatement = connection.prepareStatement("select s_id, s_name from student1 where s_id = ?")
    statement.setInt(1, 10)
    val resultSet: ResultSet = statement.executeQuery()
    while (resultSet.next()) {
      val name: String = resultSet.getString(2)
      println(s"name：${name}")
    }

  }

}
