package DataSet

import java.sql.PreparedStatement

import org.apache.flink.api.scala.ExecutionEnvironment

object MapPartition2MySql {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val sourceData: DataSet[String] = env.fromElements("3 ok", "4 hello", "5 flink")
    sourceData.mapPartition(partition => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      val conn = java.sql.DriverManager.getConnection(
        "jdbc:mysql://172.16.183.100:3306/spark",
        "root",
        "123456"
      )
      partition.map(line => {
        val statement: PreparedStatement = conn.prepareStatement(
          "insert into aaa(id, name) values (?, ?)"
        )
        statement.setInt(1, line.split(" ")(0).toInt)
        statement.setString(2, line.split(" ")(1))
        statement.execute()
      })

    }).print()
    env.execute("map partition to mysql")

  }
}
