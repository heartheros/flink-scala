import java.lang

import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}

/**
 * @author Leixinxin
 * @date 2020/7/29 6:10 PM
 */
object SplitStream {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val mainStream: DataStream[String] = env.fromElements("test", "aaa", "bbb")

    val splitStream: SplitStream[String] = mainStream.split(new OutputSelector[String] {
      override def select(value: String): lang.Iterable[String] = ???
    })
  }
}
