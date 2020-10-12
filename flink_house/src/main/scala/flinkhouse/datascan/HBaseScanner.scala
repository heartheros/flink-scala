package flinkhouse.datascan


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, Result, ResultScanner, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{Filter, FilterList, SingleColumnValueFilter}
import org.apache.hadoop.hbase.util.Bytes

object HBaseScanner {
  def main(args: Array[String]): Unit = {
    val configuration: Configuration = new Configuration
    configuration.set("hbase.zookeeper.quorum", "node01,node02,node03")
    configuration.set("hbase.zookeeper.clientPort", "2181")
    val connection = ConnectionFactory.createConnection(configuration)

    val table = connection.getTable(TableName.valueOf("flink:data_orders"))
    val scanner: Scan = new Scan()

    val start: SingleColumnValueFilter = new SingleColumnValueFilter(
      "f1".getBytes(),
      "createTime".getBytes(),
      CompareOp.GREATER_OR_EQUAL,
      "2020-10-06 04:00:00".getBytes()
    )

    val end: SingleColumnValueFilter = new SingleColumnValueFilter(
      "f1".getBytes(),
      "createTime".getBytes(),
      CompareOp.LESS_OR_EQUAL,
      "2020-10-06 05:00:00".getBytes()
    )

    val filter: FilterList = new FilterList(start, end)
    scanner.setFilter(filter)

    val records: ResultScanner = table.getScanner(scanner)

    val it = records.iterator()
    while (it.hasNext) {
      print(it.next())
      print(it.next().getValue("f1".getBytes(), "resTotalMoney".getBytes()))
      print(it.next().getValue("f1".getBytes(), "createTime".getBytes()))
    }


    table.close()
  }
}
