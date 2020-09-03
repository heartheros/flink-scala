package FlinkSql

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sinks.{CsvTableSink, TableSink}
import org.apache.flink.table.sources.CsvTableSource
/**
 * @author Leixinxin
 * @date 2020/9/2 4:36 PM
 */
object FlinkStreamSql {
  def main(args: Array[String]): Unit = {
    val streamEnvironment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    streamEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val streamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnvironment)

    val tableSource:CsvTableSource = CsvTableSource.builder()
      .field("id", Types.INT)
      .field("name", Types.STRING)
      .field("age", Types.INT)
      .ignoreFirstLine()
      .ignoreParseErrors()
      .lineDelimiter("\r\n")
      .fieldDelimiter(",")
      .path("/Users/leixinxin/Downloads/flinksql.csv")
      .build()
    streamTableEnv.registerTableSource("user", tableSource)

//    val result: Table = streamTableEnv.scan("user").filter("age > 18")
  val result: Table = streamTableEnv.sqlQuery("select `id`,`name`,`age` from `user` where id =1")

    val sink = new CsvTableSink(
      "/Users/leixinxin/Downloads/flinksql-out.csv",
      "===",
      1,
      WriteMode.OVERWRITE
    ).configure(
      Array("id", "name", "age"),
      Array(Types.INT, Types.STRING, Types.INT)
    )

    streamTableEnv.registerTableSink("flinksql-out", sink)

//    result.writeToSink(sink)
    result.insertInto("flinksql-out")

    streamEnvironment.execute()
  }
}
