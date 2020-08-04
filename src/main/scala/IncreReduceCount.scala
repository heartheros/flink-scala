import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author Leixinxin
 * @date 2020/8/4 5:11 PM
 * 需求：通过接收socket当中输入的数据，统计每5秒钟数据的累计的值
 */
object IncreReduceCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    import org.apache.flink.api.scala._

    val words: DataStream[String] = env.socketTextStream("localhost", 9000)

    val res: DataStreamSink[(Int, Int)] = words.map(x => (1, x.toInt))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
//      .sum(1)
//      .print()
      .reduce(new ReduceFunction[(Int, Int)] {
        override def reduce(t: (Int, Int), t1: (Int, Int)): (Int, Int) = {
          (t._1, t._2+t1._2)
        }
      })
      .print()

    env.execute("increment reduce")
  }
}
