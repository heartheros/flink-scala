package CustomSource

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author Leixinxin
 * @date 2020/7/29 4:35 PM
 */
object CustomParallelSourceFlinkCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val values: DataStream[String] = env.addSource(new CustomParallelSource())
    val res: DataStream[(String, Int)] = values.flatMap(x => x.split(" "))
      .map(x => (x, 1))
      .keyBy(0)
      .timeWindowAll(Time.seconds(1))
      .sum(1)

    res.print().setParallelism(2)

    env.execute("custom parallel source")
  }
}
