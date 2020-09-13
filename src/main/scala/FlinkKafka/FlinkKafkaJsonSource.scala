package FlinkKafka

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Table, _}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Json, Kafka, Schema}
import org.apache.flink.table.sinks.CsvTableSink

object FlinkKafkaJsonSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val kafka: Kafka = new Kafka()
      .startFromLatest()
      .topic("kafka_source_table2")
      .version("0.11")
      .property("group.id", "test_group")
      .property("bootstrap.servers", "node01:9092,node02:9092,node03:9092")

    val json: Json = new Json().failOnMissingField(false).deriveSchema()

    val schema: Schema = new Schema()
      .field("userId", Types.INT)
        .field("day", Types.STRING)
        .field("beginTime", Types.LONG)
        .field("endTime", Types.LONG)

    tableEnv.connect(kafka)
      .withFormat(json)
      .withSchema(schema)
      .inAppendMode()
      .registerTableSource("user_log")

    val table: Table = tableEnv.sqlQuery("select userId, `day`, beginTime, endTime from user_log")
    table.printSchema()
    val sink = new CsvTableSink(
      "/Users/leixinxin/Downloads/user-log-csv.csv",
      "==",
      1,
      WriteMode.OVERWRITE
    )

    tableEnv.registerTableSink("csvSink",
      Array[String]("f0","f1","f2","f3"),
      Array[TypeInformation[_]](Types.INT, Types.STRING, Types.LONG, Types.LONG),
      sink
    )

    table.insertInto("csvSink")
    env.execute("kafka-json-source")
//val streamEnvironment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //隐式转换
    //checkpoint配置
    /* streamEnvironment.enableCheckpointing(100);
     streamEnvironment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
     streamEnvironment.getCheckpointConfig.setMinPauseBetweenCheckpoints(500);
     streamEnvironment.getCheckpointConfig.setCheckpointTimeout(60000);
     streamEnvironment.getCheckpointConfig.setMaxConcurrentCheckpoints(1);
     streamEnvironment.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
 */
//    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnvironment)
//    val kafka: Kafka = new Kafka()
//      .version("0.11")
//      .topic("kafka_source_table2")
//      .startFromLatest()
//      .property("group.id", "test_group")
//      .property("bootstrap.servers", "node01:9092,node02:9092,node03:9092")
//
//    val json: Json = new Json().failOnMissingField(false).deriveSchema()
////    //{"userId":1119,"day":"2017-03-02","begintime":1488326400000,"endtime":1488327000000,"data":[{"package":"com.browser","activetime":120000}]}
//    val schema: Schema = new Schema()
//      .field("userId", Types.INT)
//      .field("day", Types.STRING)
//      .field("beginTime", Types.LONG)
//      .field("endTime", Types.LONG)
//    tableEnv
//      .connect(kafka)
//      .withFormat(json)
//      .withSchema(schema)
//      .inAppendMode()
//      .registerTableSource("user_log")
////    //使用sql来查询数据
//    val table: Table = tableEnv.sqlQuery("select userId,`day` ,beginTime,endTime  from user_log")
////    table.printSchema()
////    //定义sink，输出数据到哪里
//    val sink = new CsvTableSink("/Users/leixinxin/Downloads/user-log-csv.csv","====",1,WriteMode.OVERWRITE)
////    //注册数据输出目的地
//    tableEnv.registerTableSink("csvSink",
//      Array[String]("f0","f1","f2","f3"),
//      Array[TypeInformation[_]](Types.INT, Types.STRING, Types.LONG, Types.LONG),
//      sink
//    )
////    //将数据插入到数据目的地
//    table.insertInto("csvSink")
//    streamEnvironment.execute("kafkaSource")
  }
}
