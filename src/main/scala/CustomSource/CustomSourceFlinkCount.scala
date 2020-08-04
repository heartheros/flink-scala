package CustomSource

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author Leixinxin
 * @date 2020/7/29 3:34 PM
 */
object CustomSourceFlinkCount {
  def main(args: Array[String]):Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val values: DataStream[String] = env.addSource(new MySource())

    val res: DataStream[(String, Int)] = values.flatMap(x => x.split(" "))
      .map(x => (x, 1))
      .keyBy(0)
      .timeWindowAll(Time.seconds(5))
      .sum(1)

    res.print().setParallelism(1)

    env.execute("custom source")
  }
}
