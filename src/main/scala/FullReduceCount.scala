import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

/**
 * @author Leixinxin
 * @date 2020/8/4 5:47 PM
 * 需求：通过全量聚合统计，求取每3条数据的平均值
 */
object FullReduceCount {
  def main(arg: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    import org.apache.flink.api.scala._

    val numbers: DataStream[String] = env.socketTextStream("localhost", 9000)

    val res: DataStreamSink[Double] = numbers.map(x => (1, x.toInt))
      .keyBy(0)
      .countWindow(3)
      .process(new MyProcessFunctionClass)
      .print()

    env.execute()
  }
}
/**ProcessWindowFunction 需要跟四个参数
 * 输入参数类型，输出参数类型，聚合的key的类型，window的下界
 *
 */
class MyProcessFunctionClass extends ProcessWindowFunction[(Int, Int), Double, Tuple, GlobalWindow] {
  override def process(key: Tuple, context: Context, elements: Iterable[(Int, Int)], out: Collector[Double]): Unit = {

    var totalNum = 0
    var countNum = 0
    for (data <- elements) {
      totalNum += 1
      countNum += data._2
    }

    out.collect(countNum / totalNum)
  }
}