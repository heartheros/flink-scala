package flinkhouse

import java.text.DecimalFormat
import java.time.LocalDateTime
import java.util.UUID

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.jdbc.{JDBCAppendTableSink, JDBCAppendTableSinkBuilder}
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.util.serialization.{KeyedDeserializationSchema, KeyedSerializationSchema}
import org.apache.flink.types.Row

import scala.util.Random

object GenerateOrderToMysql {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val orderSource: DataStreamSource[Row] = env.addSource(
      new RichParallelSourceFunction[Row] {
        var isRunning = true
        override def run(sourceContext: SourceFunction.SourceContext[Row]): Unit = {
          while(isRunning) {
            val order: Order = generateOrder

            sourceContext.collect(
              Row.of(order.order_no, order.user_id,order.sku_id,order.sku_money,order.real_total_money,order.pay_from,order.province)
            )

            Thread.sleep(1000)
          }
        }

        override def cancel(): Unit = {
          isRunning = false

        }
      }
    )
//    orderSource.map(item => item).print()

    val mysqlSink: JDBCAppendTableSink = JDBCAppendTableSink.builder()
      .setDrivername("com.mysql.jdbc.Driver")
      .setDBUrl("jdbc:mysql://172.16.183.100:3306/product?characterEncoding=utf-8")
      .setUsername("root")
      .setPassword("123456")
      .setQuery(
          """
            |insert into orders(order_no,user_id,sku_id,sku_money,real_total_money,pay_from,province)
            |values (?,?,?,?,?,?,?)
            |""".stripMargin)
      .setBatchSize(2)
      .setParameterTypes(
        BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO
      )
      .build()

    mysqlSink.emitDataStream(orderSource)

    env.execute()
  }

  def generateOrder: Order = {
    val province: Array[String] = Array[String](
      "北京市", "天津市", "上海市", "重庆市", "河北省",
      "山西省", "辽宁省", "吉林省", "黑龙江省", "江苏省", "浙江省",
      "安徽省", "福建省", "江西省", "山东省", "河南省", "湖北省",
      "湖南省", "广东省", "海南省", "四川省", "贵州省", "云南省",
      "陕西省", "甘肃省", "青海省"
    )
    val random = new Random()

    val orderNo: String = UUID.randomUUID().toString
    val userId: Int = random.nextInt(10000)
    val skuId: Int = random.nextInt(1361)+1
    var skuMoney: Double = 100 + random.nextDouble() * 100
    skuMoney = formatMoney(skuMoney, 2).toDouble

    var realTotalMoney: Double = 150 + random.nextDouble() * 100
    realTotalMoney = formatMoney(realTotalMoney, 2).toDouble

    val payFrom = random.nextInt(2) + 1
    val provinceName: String = province(random.nextInt(province.length))

    Order(orderNo, userId+"", skuId+"", skuMoney+"", realTotalMoney+"", payFrom+"", provinceName)
  }

  def formatMoney(d: Double, newScale: Int): String = {
    var pattern = "#."
    var i = 0
    while ({i < newScale}) {
      pattern += "#"

      {
        i += 1;i - 1
      }
    }
    val df = new DecimalFormat(pattern)
    df.format(d)
  }

  case class Order(
                    order_no: String,
                    user_id: String,
                    sku_id: String,
                    sku_money: String,
                    real_total_money: String,
                    pay_from: String,
                    province: String
                  )
}
