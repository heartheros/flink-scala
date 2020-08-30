package DataSet.hbaseIntegration

import org.apache.flink.addons.hbase.TableInputFormat
import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.common.io.statistics.BaseStatistics
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.java.tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.io.InputSplitAssigner
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable, Result, Scan}
import org.apache.hadoop.hbase.util.Bytes


object FlinkReadHBase {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val res: DataSet[tuple.Tuple2[String, String]] = env.createInput(new FlinkHBaseInput)
    res.print()

    env.execute()
  }
}

class FlinkHBaseInput extends TableInputFormat[tuple.Tuple2[String, String]] {
  val zkQuorum = "node01,node02,node03"
  val zkClientPort = "2181"
  val tableName = "FlinkHbaseSource"
  val familyName = "f1"

  override def configure(parameters: Configuration): Unit = {
    val configuration = HBaseConfiguration.create()
    configuration.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum)
    configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, zkClientPort)

    val conn: Connection = ConnectionFactory.createConnection(configuration)
    table = classOf[HTable].cast(conn.getTable(TableName.valueOf(tableName)))
    scan = new Scan() {
      addFamily(Bytes.toBytes(familyName))
    }
  }

  override def getScanner: Scan = {
    scan
  }

  override def getTableName: String = {
    tableName
  }

  override def mapResultToTuple(result: Result): tuple.Tuple2[String, String] = {
    val rowKey: String = Bytes.toString(result.getRow)
    val sb = new StringBuffer()
    for (cell: Cell <- result.rawCells()) {
      val value = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
      sb.append(value + "-")
    }
    val res: String = sb.replace(sb.length()-1, sb.length(), "").toString

    val tuple2: tuple.Tuple2[String, String] = new tuple.Tuple2[String, String]()
    tuple2.setField(rowKey, 0)
    tuple2.setField(res, 1)
    tuple2
  }
}
