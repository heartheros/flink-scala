
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author Leixinxin
 * @date 2020/7/28 11:15 AM
 */
case class CountWord(word: String, count: Long)

object FlinkCount {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val result: DataStream[String] = environment.socketTextStream("localhost", 9000)

    import org.apache.flink.api.scala._

    val resultValue: DataStream[CountWord] = result.flatMap(x => x.split(" "))
      .map(x => CountWord(x, 1))
      .keyBy("word")
      /**
       * 两个参数时窗口大小 10[size]，5秒[slide]，每 5 秒统计近 10 秒的数据
       * (1,1)
       * (1,2)
       * (1,3)
       * (1,4)
       * (1,5)
       * (1,4)
       * (1,3)
       * (1,2)
       * (1,1)
       */
//      .timeWindow(Time.seconds(10), Time.seconds(5))

      /**
       * 窗口大小 5[size]，1秒[slide]滑一次，每 1 秒统计近 5 秒的数据
       * (1,1)
       * (1,2)
       * (1,3)
       * (1,4)
       * (1,5)
       * (1,4)
       * (1,3)
       * (1,2)
       * (1,1)
       */
//      .timeWindow(Time.seconds(5), Time.seconds(1))
      /**
       * 一个参数时，每5秒统计近5秒的总数
       */
      .timeWindow(Time.seconds(5)) // size
      .sum("count")

    resultValue.print().setParallelism(1)
    environment.execute()
  }
}
