import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}

/**
 * @author Leixinxin
 * @date 2020/7/29 5:54 PM
 */
object ConnectStream {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val stringStream: DataStream[String] = env.fromElements("aaa", "bbb", "ccc aaaa")

    val intStream: DataStream[Int] = env.fromElements(1,2,3,4,5,6)

    val mixStream: ConnectedStreams[String, Int] = stringStream.connect(intStream)

    val res: DataStream[Any] = mixStream.map(x => {
      x + "abc"
    }, y => {
      y * 2
    })

    res.print().setParallelism(1)

    env.execute("connect stream")
  }
}
