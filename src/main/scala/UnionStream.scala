import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @author Leixinxin
 * @date 2020/7/29 5:49 PM
 */
object UnionStream {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val firstStream: DataStream[String] = env.fromElements("aaa", "aaaa a a s dfdsfs")
    val secondStream: DataStream[String] = env.fromElements("bbb", "qicxsn a sd asdcas")

    val res: DataStream[String] = firstStream.union(secondStream).map(x => x)

    // sink 算子，打印结果
    res.print().setParallelism(1)
    env.execute()
  }
}
