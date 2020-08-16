package FlinkKafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object FlinkKafkaSource {

  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    //隐式转换
//    import org.apache.flink.api.scala._
//    //checkpoint配置
//    env.enableCheckpointing(100);
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500);
//    env.getCheckpointConfig.setCheckpointTimeout(60000);
//    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1);
//    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//    val topic = "test"
//
//    val prop = new Properties()
//    prop.setProperty("bootstrap.servers","node01:9092")
//    prop.setProperty("group.id","con1")
//    prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//    prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//
//    var kafkaSoruce: FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), prop)
//
//    kafkaSoruce.setCommitOffsetsOnCheckpoints(true)
//    //设置statebackend
//    env.setStateBackend(new RocksDBStateBackend("hdfs://node01:8020/flink_kafka/checkpoints",true));
//    val result: DataStream[String] = env.addSource(kafkaSoruce)
//    result.print()
//    env.execute()

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    env.enableCheckpointing(100)

    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(
      CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    )

    val topic = "test"
    val props = new Properties()
    props.setProperty("bootstrap.servers", "node01:9092")
    props.setProperty("group.id", "con1")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    var kafkaSource: FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String](
      topic,
      new SimpleStringSchema(),
      props
    )

    env.setStateBackend(
      new RocksDBStateBackend(
        "hdfs://node01:8020/flink/checkDir",
        true
      )
    )

    val result: DataStream[(String,Int)] = env.addSource(kafkaSource)
        .flatMap(x => x.split(" "))
        .map(x => (x, 1))
        .keyBy(0)
        .sum(1)
    result.print()
    env.execute("kafka-source")
  }
}
