package Retail

import java.io.FileInputStream
import java.util.Properties

import scala.concurrent.Future
import io.vertx.core.json.JsonObject
import io.vertx.lang.scala.VertxExecutionContext
import io.vertx.scala.core.{Vertx, VertxOptions}
import io.vertx.scala.ext.jdbc.JDBCClient
import io.vertx.scala.ext.sql.{ResultSet, SQLClient}
import redis.clients.jedis.Jedis

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

/**
 * @author Leixinxin
 * @date 2020/11/9 3:42 PM
 */
object TestReadConfig {
  def main(args: Array[String]): Unit = {
    var globalConfig = new Properties()
//    var sqlConfig: Properties = _
//    var vertx: Vertx = _
    globalConfig.load(
      new FileInputStream(Thread.currentThread()
        .getContextClassLoader
        .getResource("config.properties")
        .getPath)
    )
    val vertx = Vertx.vertx(VertxOptions().setWorkerPoolSize(10).setEventLoopPoolSize(5))

    val config: JsonObject = new JsonObject()
      .put("max_pool_size", 20)
      .put("driver_class", "com.mysql.jdbc.Driver")
      .put("user", globalConfig.getProperty("mysql.user"))
      .put("password", globalConfig.getProperty("mysql.password"))
    config.put("url", globalConfig.getProperty("mysql.url"))
    val sqlClient = JDBCClient.createShared(vertx, config)

    sqlClient.getConnectionFuture().onComplete{
      case Success(result) => {

        var connection = result

        connection.queryFuture("SELECT * FROM sss").onComplete{
          case Success(result) => {

            var rs = result
            rs.getResults.foreach(line => {
//              println(line.getString(1))
              println(line)
            })
            connection.close()
          }
          case Failure(cause) => {
            println(s"$cause")
            connection.close()
          }
        }
      }
      case Failure(cause) => {
        println(s"$cause")
      }
    }(VertxExecutionContext(vertx.getOrCreateContext()))




  }
}
