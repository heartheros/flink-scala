package Retail


import java.io.FileInputStream
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import io.vertx.core.Promise.promise
import io.vertx.core.json.JsonObject
import io.vertx.lang.scala.VertxExecutionContext
import io.vertx.scala.core.{Vertx, VertxOptions}
import io.vertx.scala.ext.sql.SQLClient

import scala.util.{Failure, Success}
//import io.vertx.ext.jdbc.JDBCClient
import io.vertx.scala.ext.jdbc.JDBCClient
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer


class MysqlDimensionAsyncFunction extends RichAsyncFunction[BinLogObject, BinLogObject] {

  var dbProps: Properties = _
  var jedisCon: Jedis = _
  var sqlClient: SQLClient = _
  var vertx: Vertx = _

  var dimensionTables: Set[String] = _
  var cacheTables: Set[String] = _
  var realTimeTables: Set[String] = _

  override def open(parameters: Configuration): Unit = {

    dbProps = new Properties()
    dbProps.load(
      new FileInputStream(Thread.currentThread()
        .getContextClassLoader
        .getResource("config.properties")
        .getPath)
    )
    dimensionTables = dbProps.getProperty("tables.dimension").split(",").toSet
    cacheTables = dbProps.getProperty("tables.cache").split(",").toSet
    realTimeTables = dbProps.getProperty("tables.realtime").split(",").toSet

    jedisCon = new Jedis("redis.host", 6379)
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

    val line: JSONObject = JSON.parseObject(input.data)

    if (dimensionTables.contains(table) && !opType.equalsIgnoreCase("insert")) {
      setRedisExpire(getRedisKey(database, table, line.get("id").toString.toInt))
    }

    if (cacheTables.contains(table)) {
    }

    if (realTimeTables.contains(table)) {

      var dimensionSqlListBuffer: ListBuffer[String] = ListBuffer();
      dimensionSqlListBuffer += getSql(
        table,
        line.get("aaa").toString.toInt,
        "a,b".split(","),
        "aaa-name"
      )
      dimensionSqlListBuffer += getSql(
        table,
        line.get("bbb").toString.toInt,
        "c,d".split(","),
        "bbb-name"
      )

      genMysqlConn(input.database)
      sqlClient.getConnectionFuture().onComplete{
        case Success(result) => {
          var connection = result

          connection.queryFuture(dimensionSqlListBuffer.mkString(" union all ")).onComplete{
            case Success(result) => {
              print(result)
              // Do something with results
            }
            case Failure(cause) => println("Failure")
          }(VertxExecutionContext(vertx.getOrCreateContext()))
        }
        case Failure(cause) =>
          println(s"$cause")
      }(VertxExecutionContext(vertx.getOrCreateContext()))
    }
  }

  def getSql(table: String, index: Int, fields: Array[String], indexFieldName: String): String = {
    "select " + table + "as t," + fields.mkString(",") + " from " + table + " where " + indexFieldName + "=" + index
  }

  def genMysqlConn(database: String): Unit = {
    vertx = Vertx.vertx(VertxOptions().setWorkerPoolSize(10).setEventLoopPoolSize(5))

    val config: JsonObject = new JsonObject()
      .put("driver_class", "com.mysql.jdbc.Driver")
      .put("max_pool_size", 20)
      .put("user", dbProps.getProperty("mysql.user"))
      .put("password", dbProps.getProperty("mysql.password"))
    config.put("url", dbProps.getProperty("mysql.url") + database)
    sqlClient = JDBCClient.createShared(vertx, config, database)
  }

  def getRedisCacheWithExpire(redisKey: String): String = {
    jedisCon.get(redisKey)
  }

  def setRedisCacheWithExpire(redisKey: String, ttl: Int, value: String): Boolean = {
    if (null == jedisCon.setex(redisKey, ttl, value)) {
      return false
    }
    true
  }

  def setRedisExpire(redisKey: String): Unit = {
    jedisCon.expire(redisKey, -1)
  }

  def getRedisKey(database: String, table: String, id: Int): String = {
    database + ":" + table + ":" + id.toString
  }
}
