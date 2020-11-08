package flinkhouse

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.conf
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Delete, Put, Table}
import redis.clients.jedis.Jedis

class HBaseSinkFunction extends RichSinkFunction[OrderObj] {
  var connection: Connection = _
  var hTable: Table = _
  var redisCon: Jedis = _

  override def open(parameters: Configuration): Unit = {
    val configuration: conf.Configuration = HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.quorum", "node01,node02,node03")
    configuration.set("hbase.zookeeper.property.clientPort", "2181")
    connection = ConnectionFactory.createConnection(configuration)
    hTable = connection.getTable(TableName.valueOf("flink:data_orders"))

    redisCon = new Jedis("localhost", 6379)
  }

  override def close(): Unit = {
    if (null != hTable) {
      hTable.close()
    }
    if (null != connection) {
      connection.close()
    }

    if (null != redisCon) {
      redisCon.close()
    }
  }

  def insertHbase(htable: Table, orderObj: OrderObj): Unit = {
    val database: String = orderObj.database
    val table: String = orderObj.table
    val data: String = orderObj.data

    val orderInfo: JSONObject = JSON.parseObject(data)
    val id = orderInfo.get("id").toString
    val order_no = orderInfo.get("order_no").toString
    val user_id = orderInfo.get("user_id").toString
    val sku_id = orderInfo.get("sku_id").toString
    val sku_money = orderInfo.get("sku_money").toString
    val real_total_money = orderInfo.get("real_total_money").toString
    val pay_from = orderInfo.get("pay_from").toString
    val province = orderInfo.get("province").toString
    val create_time = orderInfo.get("create_time").toString

    val put: Put = new Put(id.getBytes())
    put.addColumn("f1".getBytes(), "orderNo".getBytes(), order_no.getBytes())
    put.addColumn("f1".getBytes(), "userId".getBytes(), user_id.getBytes())
    put.addColumn("f1".getBytes(), "skuId".getBytes(), sku_id.getBytes())
    put.addColumn("f1".getBytes(), "skuMoney".getBytes(), sku_money.getBytes())
    put.addColumn("f1".getBytes(), "realTotalMoney".getBytes(), real_total_money.getBytes())
    put.addColumn("f1".getBytes(), "payFrom".getBytes(), pay_from.getBytes())
    put.addColumn("f1".getBytes(), "province".getBytes(), province.getBytes())
    put.addColumn("f1".getBytes(), "createTime".getBytes(), create_time.getBytes())

    hTable.put(put)
  }

  def deleteHbase(hTable: Table, orderObj: OrderObj): Unit = {
    val orderInfo: JSONObject =  JSON.parseObject(orderObj.data)
    val rowKey: String = orderInfo.get("id").toString
    val delete: Delete = new Delete(rowKey.getBytes())
    hTable.delete(delete)
  }

  def redisKeyExpire(dimensionObj: OrderObj): Unit = {
    val orderInfo: JSONObject =  JSON.parseObject(dimensionObj.data)
    val dimensionTableId = orderInfo.get("id").toString
    val dimensionTableName = dimensionObj.table
    val dimensionTableDbName = dimensionObj.database
    redisCon.expire(dimensionTableDbName + ":" + dimensionTableName + ":" + dimensionTableId, 1)
  }

  def redisKeyGet(id: String, tableName: String, dbName: String): String = {
    redisCon.get(dbName + ":" + tableName + ":" + id)
  }

  override def invoke(value: OrderObj, context: SinkFunction.Context[_]): Unit = {
    val database: String = value.database
    val table: String = value.table
    val typeResult = value.`type`
    if (table.equalsIgnoreCase("orders")
      && database.equalsIgnoreCase("product")) {
      if (typeResult.equalsIgnoreCase("insert")) {
        insertHbase(hTable, value)
      } else if (typeResult.equalsIgnoreCase("update")) {
        insertHbase(hTable, value)
      } else if (typeResult.equalsIgnoreCase("delete")) {
        deleteHbase(hTable, value)
      }
    } else if (table.equalsIgnoreCase("abc")
      || table.equalsIgnoreCase("cba")) {
      redisCon.ttl()
    }
  }
}
