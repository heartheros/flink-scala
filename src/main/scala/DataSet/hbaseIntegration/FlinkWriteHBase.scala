package DataSet.hbaseIntegration

import java.util

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HConstants, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, BufferedMutator, BufferedMutatorParams, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

object FlinkWriteHBase {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val dataSource: DataSet[String] = env.readTextFile("hdfs://node01/cde.log")

    dataSource.output(new FlinkHBaseOutput)

    env.execute("from hdfs to hbase")
  }
}

class FlinkHBaseOutput extends OutputFormat[String] {
  var conn: Connection = null
  var admin: Admin = null
  val zkQuorum = "node01,node02,node03"
  val zkClientPort = "2181"
  override def configure(configuration: Configuration): Unit = {

  }

  override def open(i: Int, i1: Int): Unit = {
    val configuration: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create()
    configuration.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum)
    configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, zkClientPort)
    configuration.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000)
    configuration.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000)

    conn = ConnectionFactory.createConnection(configuration)
    admin = conn.getAdmin
  }

  override def writeRecord(it: String): Unit = {
    val tableName: TableName = TableName.valueOf("FlinkHbaseSource")
    val familyName = "f1"

    if (!admin.tableExists(tableName)) {
      val hTableDescriptor: HTableDescriptor = new HTableDescriptor(tableName)
      hTableDescriptor.addFamily(new HColumnDescriptor(familyName))
      admin.createTable(hTableDescriptor)
    }

    val tmpLine: Array[String] = it.split(",")
    val put: Put = new Put(Bytes.toBytes(tmpLine(0)))

    put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("name"), Bytes.toBytes(tmpLine(1)))
    put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("age"), Bytes.toBytes(tmpLine(2)))

    val putList: util.ArrayList[Put] = new util.ArrayList[Put]()
    putList.add(put)

    val bufferedMutatorParams: BufferedMutatorParams = new BufferedMutatorParams(tableName)
    bufferedMutatorParams.writeBufferSize(1024*1024)
    val bufferedMutator: BufferedMutator = conn.getBufferedMutator(bufferedMutatorParams)

    bufferedMutator.mutate(putList)
    bufferedMutator.flush()
    putList.clear()
  }

  override def close(): Unit = {
    if (null != conn) {
      conn.close()
    }
  }
}
