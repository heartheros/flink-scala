package Retail


import java.io.FileInputStream
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import io.vertx.core.Promise.promise
import io.vertx.core.json.{JsonArray, JsonObject}
import io.vertx.lang.scala.VertxExecutionContext
import io.vertx.scala.core.{Future, Promise, Vertx, VertxOptions}
import io.vertx.scala.ext.sql.{ResultSet, SQLClient}

import scala.util.{Failure, Success}
//import io.vertx.ext.jdbc.JDBCClient
import io.vertx.scala.ext.jdbc.JDBCClient
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer


class MysqlDimensionAsyncFunction extends RichAsyncFunction[BinLogObject, BinLogObject] {

  var globalConfig: Properties = _
  var sqlConfig: Properties = _
  var jedisCon: Jedis = _
  var sqlClient: SQLClient = _
  val vertx: Vertx = Vertx.vertx(VertxOptions().setWorkerPoolSize(10).setEventLoopPoolSize(5))
  val config: JsonObject = new JsonObject()
    .put("driver_class", "com.mysql.jdbc.Driver")
    .put("max_pool_size", 20)
    .put("user", globalConfig.getProperty("mysql.user"))
    .put("password", globalConfig.getProperty("mysql.password"))
  var dimensionTables: Set[String] = _
  var cacheTables: Set[String] = _
  var realTimeTables: Set[String] = _

  var aTypes: Set[String] = _
  var bTypes: Set[String] = _

  override def open(parameters: Configuration): Unit = {
    globalConfig = new Properties()
    globalConfig.load(
      new FileInputStream(Thread.currentThread()
        .getContextClassLoader
        .getResource("config.properties")
        .getPath)
    )

    dimensionTables = globalConfig.getProperty("tables.dimension").split(",").toSet
    cacheTables = globalConfig.getProperty("tables.cache").split(",").toSet
    realTimeTables = globalConfig.getProperty("tables.realtime").split(",").toSet

    sqlConfig = new Properties()
    sqlConfig.load(
      new FileInputStream(Thread.currentThread()
        .getContextClassLoader
        .getResource("sql.properties")
        .getPath))
    aTypes = sqlConfig.getProperty("dimension.a.types").split(",").toSet
    bTypes = sqlConfig.getProperty("dimension.b.types").split(",").toSet

    jedisCon = new Jedis(
      globalConfig.getProperty("redis.host"),
      globalConfig.getProperty("redis.port").toInt
    )
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
      setRedisExpire(getRedisKey(database, table, line.get("id").toString))
    }

    if (cacheTables.contains(table)) {
    }

    if (realTimeTables.contains(table)) {
      val dimensionSql: String = getDimensionSql(database, table, line)
      if (null != dimensionSql) {
        genMysqlClient(input.database)

        // 获取 id
        val infoId: Future[Int] = infoQuery(line.getIntValue("id"))

        // 根据 id 获取品牌
        val brandString: Future[String] = infoId.compose(infoId => brandQuery(infoId))

        // 根据品牌获取单位
        val unitId: Future[Int] = brandString.compose(brandString => unitQuery(brandString))


//        val testConnectionFuture = sqlClient.getConnectionFuture()

//        testConnectionFuture.onComplete {
//          case Success(result) => {
//            var connection = result
//
//            connection.queryFuture(dimensionSql).onComplete {
//              case Success(result) => {
//                print(result)
//                // Do something with results
//              }
//              case Failure(cause) => println("Failure")
//            }(VertxExecutionContext(vertx.getOrCreateContext()))
//          }
//          case Failure(cause) =>
//            println(s"$cause")
//        }(VertxExecutionContext(vertx.getOrCreateContext()))
      }
    }
  }

  def infoQuery(goodsId: Int): Future[Int] = {
    val promise: Promise[Int] = Promise.promise()
    sqlClient.getConnection(ar => {
      if (ar.failed()) {
        promise.fail(ar.cause())
        return null
      }
      // 获取连接成功
      var connection = ar.result()
      connection.querySingle("select info", result => {
        promise.complete(result.result().get.getInteger(0))
      })
    })
    sqlClient.close()
    promise.future()
  }

  def brandQuery(goodsId: Int): Future[String] = {
    val promise: Promise[String] = Promise.promise()
    if (goodsId == 1) {
      sqlClient.getConnection(ar => {
        if (ar.failed()) {
          promise.fail(ar.cause())
          return null
        }
        var connection = ar.result()
        connection.querySingle("select brand", result => {
          if (result.succeeded()) {
            promise.complete(result.result().get.getString(0))
          }
        })
      })
      sqlClient.close()
    } else {
      promise.complete("asdadsa")
    }

    promise.future()
  }

  def unitQuery(goodsId: String): Future[Int] = {
    val promise: Promise[Int] = Promise.promise()
    sqlClient.getConnection(ar => {
      if (ar.failed()) {
        promise.fail(ar.cause())
        return null
      }
      var connection = ar.result()
      connection.querySingle("select unit", result => {
        if (result.succeeded()) {
          promise.complete(result.result().get.getInteger(0))
        }
      })
    })
    sqlClient.close()
    promise.future()
  }

  def getDimensionSql(database: String, table: String, line: JSONObject): String = {
    val typeName = sqlConfig.getProperty(table)
    var dimensionSqlListBuffer: ListBuffer[String] = ListBuffer();

    sqlConfig.getProperty("dimension." + typeName + ".types").split(",").foreach(dimensionTableName => {
      val rawSql = sqlConfig.getProperty("dimension.r." + typeName + ".sql." + dimensionTableName)

//      dimension.a.types=info,category,brand,unit,channel,channel-name,material,mwac,wac
//      dimension.b.types=info,category,channel,channel-name,material,mwac,wac

      val index = line.get("dimension.r." + typeName + ".sql." + dimensionTableName + ".index").toString
      if (null == getRedisCache(getRedisKey(database, dimensionTableName, index))) {
//        val sql: Unit =
        dimensionSqlListBuffer += printf(rawSql, index).toString
      }
    })


    if (0 == dimensionSqlListBuffer.size) {
      return null
    }
    dimensionSqlListBuffer.mkString(" union all")
  }

  def getSql(table: String, index: Int, fields: Array[String], indexFieldName: String): String = {
    "select " + table + "as t," + fields.mkString(",") + " from " + table + " where " + indexFieldName + "=" + index
  }

  def genMysqlClient(database: String): Unit = {

    config.put("url", globalConfig.getProperty("mysql.url") + database)
    sqlClient = JDBCClient.createShared(vertx, config, database)
  }

  def getRedisCache(redisKey: String): String = {
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

  def getRedisKey(database: String, table: String, id: String): String = {
    database + ":" + table + ":" + id
  }
}
