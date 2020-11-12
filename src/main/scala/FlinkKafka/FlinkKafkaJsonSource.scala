package FlinkKafka

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Json, Kafka, Schema}
import org.apache.flink.table.sinks.CsvTableSink

object FlinkKafkaJsonSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val kafka: Kafka = new Kafka()
        .startFromLatest()
        .version("0.11")
        .topic("kafka_source_table2")
        .property("group.id", "kafka-json-source-csv-sink")
        .property("bootstrap.servers", "node01:9092,node02:9092,node03:9092")

    val json: Json = new Json().deriveSchema().failOnMissingField(false)

    val schema: Schema = new Schema()
        .field("userId", Types.INT)
        .field("day", Types.STRING)
        .field("beginTime", Types.LONG)
        .field("endTime", Types.LONG)

    tableEnv.connect(kafka)
      .withSchema(schema)
      .withFormat(json)
      .inAppendMode()
      .registerTableSource("user_log")

    val table: Table = tableEnv.sqlQuery("select userId,`day`,beginTime, endTime from user_log")

    val csvSink: CsvTableSink = new CsvTableSink(
      "/Users/luckychacha/Downloads/20200915-user-log.csv",
      "====",
      1,
      WriteMode.OVERWRITE
    )

    tableEnv.registerTableSink(
      "user_log_sink",
      Array[String]("f0","f1","f2","f3"),
      Array[TypeInformation[_]](Types.INT,Types.STRING,Types.LONG,Types.LONG),
      csvSink
    )

    table.insertInto("user_log_sink")
    env.execute("kafka-json-source-csv-sink")
  }
}
