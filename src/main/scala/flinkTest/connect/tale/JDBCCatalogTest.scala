package flinkTest.connect.tale

import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.connector.jdbc.catalog.JdbcCatalog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * @Auther: wxf
 * @Date: 2022/11/14 16:47:05
 * @Description: JDBCCatalogTest  Catalog 的使用
 * @Version 1.0.0
 */
object JDBCCatalogTest {
  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 设置 JDBC Catalog
    // Scala 方式
    val cataName: String = "my_catalog"
    val defaultDatabase: String = "db"
    val username: String = "root"
    val password: String = "root"
    val baseUrl: String = "jdbc:mysql://localhost:3306"
    val jdbcCatalog: JdbcCatalog = new JdbcCatalog(cataName, defaultDatabase, username, password, baseUrl)
    // 注册 Catalog    1.15 之后才支持
    tableEnv.registerCatalog(cataName, jdbcCatalog)

    // 设置 JdbcCatalog 为会话的当前 catalog
    tableEnv.useCatalog(cataName)
    tableEnv.listDatabases()
    tableEnv.listTables()

  }
}