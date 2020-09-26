package flinkhouse

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopOutputFormat
import org.apache.flink.types.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.client.{Mutation, Put}
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job

object ImportSkuToHBase {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
        .setDBUrl("jdbc:mysql://172.16.183.100:3306/product")
        .setDrivername("com.mysql.driver.jdbc")
        .setPassword("123456")
        .setUsername("root")
        .setQuery("select * from sku")
        .setRowTypeInfo(new RowTypeInfo(
          BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,
          BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,
          BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,
          BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,
        ))
      .setFetchSize(2)
        .finish()

    val skus: DataSet[Row] = env.createInput(jdbcInputFormat)
    val result: DataSet[(Text, Mutation)] = skus.map(sku => {
      val rowKey = new Text(sku.getField(0).toString)
      val put: Put = new Put(sku.getField(0).toString.getBytes())
      put.addColumn("f1".getBytes(), "name".getBytes(), sku.getField(1).toString.getBytes())
      put.addColumn("f1".getBytes(), "sell_price".getBytes(), sku.getField(2).toString.getBytes())
      put.addColumn("f1".getBytes(), "product_pic".getBytes(), sku.getField(3).toString.getBytes())
      put.addColumn("f1".getBytes(), "product_brand".getBytes(), sku.getField(4).toString.getBytes())
      put.addColumn("f1".getBytes(), "product_fbl".getBytes(), sku.getField(5).toString.getBytes())
      put.addColumn("f1".getBytes(), "product_code".getBytes(), sku.getField(6).toString.getBytes())
      put.addColumn("f1".getBytes(), "product_url".getBytes(), sku.getField(7).toString.getBytes())
      put.addColumn("f1".getBytes(), "product_source".getBytes(), sku.getField(8).toString.getBytes())
      put.addColumn("f1".getBytes(), "sku_stock".getBytes(), sku.getField(9).toString.getBytes())
      put.addColumn("f1".getBytes(), "appraise_count".getBytes(), sku.getField(10).toString.getBytes())
      (rowKey, put.asInstanceOf[Mutation])
    })

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "node01,node02,node03")
    hbaseConf.set(HConstants.CLIENT_PORT_STR, "2181")
    hbaseConf.set("mapred.output.dir", "/tmp2")

    val job = Job.getInstance(hbaseConf)

    result.output(
      new HadoopOutputFormat[Text, Mutation](
        new TableOutputFormat[Text], job
      )
    )

    env.execute("load data from mysql to hbase")
  }
}
