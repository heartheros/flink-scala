import java.{lang, util}
import java.util.ArrayList

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
//    val mainStream: DataStream[String] = env.fromElements("test", "aaa", "bbb", "test666", "hjlkdsajlk test")

    val mainStream: DataStream[String] = env.socketTextStream("localhost", 9000)
    val splitStream: SplitStream[String] = mainStream.split(new OutputSelector[String] {
      override def select(value: String): lang.Iterable[String] = {
        val strings = new util.ArrayList[String]()
        if (value.contains("test")) {
          strings.add("test")
        } else {
          strings.add("others")
        }
        strings
      }
    })

    val testStream: DataStream[String] = splitStream.select("test")
    val otherStream: DataStream[String] = splitStream.select("others")

    testStream.flatMap(x => x.split(" "))
      .map(x=>(x,1))
      .keyBy(0)
      .sum(1)
      .print().setParallelism(1)

    otherStream.print().setParallelism(1)
    env.execute()
  }
}
