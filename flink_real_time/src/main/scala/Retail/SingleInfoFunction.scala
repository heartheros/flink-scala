package Retail

import com.alibaba.fastjson.serializer.SerializerFeature
import io.vertx.core.json.JsonArray
import io.vertx.scala.ext.sql.SQLConnection
import com.alibaba.fastjson.{JSON, JSONObject}
import io.vertx.scala.core.Promise
import redis.clients.jedis.Jedis

import scala.beans.BeanProperty

/**
 * @author Luckychacha
 * @date 2020/11/20 11:49 AM
 */
class SingleInfoFunction(jedisCon: Jedis) extends RetailFunction(jedisCon) {
}