package FlinkSql

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment

object FlintStreamTableTransfer {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    import org.apache.flink.api.scala._
    import org.apache.flink.table.api.scala._

    val streamSource: DataStream[String] = env.socketTextStream(
      "localhost",
      9000
    )

    tableEnv.registerDataStream("userInfo", streamSource)

    val userTable: Table = tableEnv.sqlQuery("select * from userInfo")

    val appendStream: DataStream[User] = tableEnv.toAppendStream[User](userTable)
    val retractStream: DataStream[(Boolean, User)] = tableEnv.toRetractStream[User](userTable)
  }
}

class User (id: Int, age: Int)
