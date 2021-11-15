package day05

import java.sql.Connection
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource

object DruidConnectionPool {

  private val props = new Properties()

  props.put("driverClassName", "com.mysql.jdbc.Driver")
  props.put("url", "jdbc:mysql://localhost:3306/mysql_study?characterEncoding=UTF-8&useSSL=false")
  props.put("username", "root")
  props.put("password", "123")

  private val source: DataSource = DruidDataSourceFactory.createDataSource(props)

  def getConnection: Connection = {
    source.getConnection
  }
}
