package Retail

import java.io.FileInputStream
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import io.vertx.core.json.JsonObject
import io.vertx.scala.core.{Future, Promise, Vertx, VertxOptions}
import io.vertx.scala.ext.jdbc.JDBCClient
import io.vertx.scala.ext.sql.{ResultSet, SQLClient}
import redis.clients.jedis.Jedis

import scala.beans.BeanProperty

/**
 * @author Leixinxin
 * @date 2020/11/9 3:42 PM
 */
object TestReadConfig {

  var globalConfig: Properties = _
  var sqlClient: SQLClient = _
  val vertx: Vertx = Vertx.vertx(VertxOptions().setWorkerPoolSize(10).setEventLoopPoolSize(5))
  var config: JsonObject = _
  var jedisCon: Jedis = _

  def main(args: Array[String]): Unit = {
    globalConfig = new Properties()
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
    jedisCon = new Jedis(
      globalConfig.getProperty("redis.host"),
      globalConfig.getProperty("redis.port").toInt
    )

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

  def getRedisKey(database: String, table: String, id: String): String = {
    database + ":" + table + ":" + id
  }
}

