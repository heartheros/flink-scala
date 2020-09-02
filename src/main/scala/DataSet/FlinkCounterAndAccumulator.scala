package DataSet

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

object FlinkCounterAndAccumulator {
  def main(args: Array[String]) : Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val source: DataSet[String] = env.readTextFile("/Users/leixinxin/Downloads/catalina.out")

    source.map(new RichMapFunction[String, String] {
      val counter = new LongCounter()

      override def open(parameters: Configuration): Unit = {
        getRuntimeContext.addAccumulator("test-accumulator", counter)
      }
      override def map(in: String): String = {
        if (in.toLowerCase.contains("exception")) {
          counter.add(1)
        }
        in
      }
    }).setParallelism(4).writeAsText("/Users/leixinxin/Downloads/catalina-out-count.log")

    val job = env.execute("accumulator")

    val count = job.getAccumulatorResult[Long]("test-accumulator")

    println(count)
  }
}
