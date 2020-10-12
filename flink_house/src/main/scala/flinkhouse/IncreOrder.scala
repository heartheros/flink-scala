package flinkhouse

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011


object IncreOrder {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(
      CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    )
    env.setStateBackend(
      new RocksDBStateBackend("hdfs://node01:8020/flink_kafka/checkpoints", true)
    )

    val props = new Properties
    props.put("bootstrap.servers", "node01:9092,node02:9092,node03:9092")
    props.put("zookeeper.connect", "node01:2181,node02:2181,node03:2181")
    props.put("group.id", "flinkHouseGroup")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("flink.partition-discovery.interval-millis", "30000")

    val kafkaSource = new FlinkKafkaConsumer011[String](
      "flink_house",
      new SimpleStringSchema(),
      props
    )
    kafkaSource.setCommitOffsetsOnCheckpoints(true)
    val result: DataStream[String] = env.addSource(kafkaSource)

    val orderResult: DataStream[OrderObj] = result.map(line => {
      val jsonObj: JSONObject = JSON.parseObject(line)
      val database: AnyRef = jsonObj.get("database")
      val table: AnyRef = jsonObj.get("table")
      val `type`: AnyRef = jsonObj.get("type")
      val string: String = jsonObj.get("data").toString

      OrderObj(database.toString, table.toString, `type`.toString, string)
    })

    orderResult.addSink(new HBaseSinkFunction)
    env.execute()
  }
}

case class OrderObj(database:String, table:String, `type`: String, data: String) extends Serializable
