import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object FlinkPartition {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val values: DataStream[String] = env.fromElements("hello aaa", "hello bbb", "great ccc")

    val resultStream: DataStream[(String, Int)] = values.filter(x => x.contains("hello"))
      .rebalance
      .flatMap(x => x.split(" "))
      .map(x => (x, 1))
      .keyBy(0)
      .sum(1)

    resultStream.print().setParallelism(1)

    env.execute("rebalance")
  }
}
