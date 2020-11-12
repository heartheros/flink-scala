package flinkhouse

import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.types.Row

object ImportSkuToMysql {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val csvFilePath = "/Users/luckychacha/Downloads/Flink实时数仓/数据/实时数仓建表以及数据/kaikeba_goods.csv";
    val fileSource = env.readTextFile(csvFilePath)
    val lines: DataSet[String] = fileSource.flatMap(line => {
      line.split("\r\n")
    })
    val rows: DataSet[Row] = lines.map(line => {
      val fields: Array[String] = line.split("===")
      //数组是从 0 开始的。
      Row.of(
        null, fields(1), fields(2), fields(3), fields(4), fields(5), fields(6),
        fields(7), fields(8), fields(9), fields(10)
      )
    })

    rows.output(
      JDBCOutputFormat.buildJDBCOutputFormat()
        .setDBUrl("jdbc:mysql://172.16.183.100:3306/product?characterEncoding=utf-8")
        .setBatchInterval(2)
        .setDrivername("com.mysql.jdbc.Driver")
        .setPassword("123456")
        .setUsername("root")
        .setQuery(
          """insert into
            |sku
            |(id,name,sell_price,product_pic,product_brand,product_fbl,
            |product_code,product_url,product_source,
            |sku_stock,appraise_count)
            |values
            |(?,?,?,?,?,?,?,?,?,?,?)""".stripMargin
        )
        .finish()
    )



    env.execute()



  }
}
