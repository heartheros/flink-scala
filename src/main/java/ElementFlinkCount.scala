import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @author Leixinxin
 * @date 2020/7/29 2:53 PM
 */
object ElementFlinkCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val values: DataStream[String] = env.fromElements[String]("aaa aaaa", "hehe", "lxx")

    val res: DataStream[(String, Int)] = values.flatMap(x => x.split(" "))
      .map(x => (x, 1))
      .keyBy(0)
      .sum(1)

    res.print().setParallelism(1)

    env.execute("from element")
  }
}
