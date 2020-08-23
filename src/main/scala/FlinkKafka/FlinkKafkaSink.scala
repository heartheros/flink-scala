package FlinkKafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/**
 * @author Leixinxin
 * @date 2020/8/18 3:11 PM
 */
object FlinkKafkaSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(
      CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    )
    env.setStateBackend(
      new RocksDBStateBackend("hdfs://localhost:8020/flink_kafka_sink/checkpoints")
    )

    val sourceTopic = "TestSource"
    val sourceProps = new Properties()
    sourceProps.setProperty("bootstrap.servers", "localhost:9092")
    sourceProps.setProperty("group.id", "con1")
    sourceProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    sourceProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    var kafkaSource: FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String](
      sourceTopic,
      new SimpleStringSchema(),
      sourceProps
    )

    val sinkTopic = "TestSink"
    val sinkProperties = new Properties()
    sinkProperties.setProperty("bootstrap.servers", "localhost:9092")
    sinkProperties.setProperty("group.id", "con2")
    sinkProperties.setProperty("transaction.timeout.ms", 60000*15+"")

    val kafkaSink = new FlinkKafkaProducer011[String](
      sinkTopic,
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
      sinkProperties,
      FlinkKafkaProducer011.Semantic.EXACTLY_ONCE
    )

    env.addSource(kafkaSource)
      .flatMap(x => x.split(" "))
      .map(x => x.toUpperCase())
      .addSink(kafkaSink)

    env.execute("from kafka to kafka ")





  }
}
