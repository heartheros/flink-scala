package Retail


import java.io.FileInputStream
import java.util.Properties

import io.vertx.core.json.JsonObject
import io.vertx.core.{Vertx, VertxOptions}
import io.vertx.ext.jdbc.JDBCClient
import io.vertx.ext.sql.SQLClient
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import redis.clients.jedis.Jedis


class MysqlDimensionAsyncFunction extends RichAsyncFunction[BinLogObject, BinLogObject] {

  var dbProps: Properties = _
  var jedisCon: Jedis = _
  var sqlClient: SQLClient = _

  var dimensionTables: Set[String] = _
  var cacheTables: Set[String] = _
  var realTimeTables: Set[String] = _

  override def open(parameters: Configuration): Unit = {
    val vertx: Vertx = Vertx.vertx(
      new VertxOptions()
        .setWorkerPoolSize(10)
        .setEventLoopPoolSize(5)
    )
    dbProps = new Properties()
    dbProps.load(
      new FileInputStream(Thread.currentThread()
        .getContextClassLoader
        .getResource("mysql.properties")
        .getPath)
    )
    dimensionTables = dbProps.getProperty("tables.dimension").split(",").toSet
    cacheTables = dbProps.getProperty("tables.cache").split(",").toSet
    realTimeTables = dbProps.getProperty("tables.realtime").split(",").toSet

    val config: JsonObject = new JsonObject()
        .put("url", dbProps.getProperty("mysql.url"))
        .put("driver_class", "com.mysql.jdbc.Driver")
        .put("max_pool_size", 20)
        .put("user", dbProps.getProperty("mysq.user"))
        .put("password", dbProps.getProperty("mysql.password"))

    sqlClient = JDBCClient.createShared(vertx, config)

    jedisCon = new Jedis("localhost", 6379)
  }

  override def close(): Unit = {
    super.close()
    if (null != sqlClient) {
      sqlClient.close()
    }
    if (null != jedisCon) {
      jedisCon.close()
    }
  }

  override def asyncInvoke(input: BinLogObject, resultFuture: ResultFuture[BinLogObject]): Unit = {
    val database: String = input.database
    val table: String = input.table
    val opType: String = input.`type`

    if (dimensionTables.contains(table)) {
    }

    if (cacheTables.contains(table)) {
    }

    if (realTimeTables.contains(table)) {
    }
  }
}
