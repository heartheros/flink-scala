import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem

/**
 * @author Leixinxin
 * @date 2020/7/28 2:42 PM
 */
object BatchFlinkCount {
  def main(args: Array[String]): Unit = {
    val input = "/Users/leixinxin/Sites/input.txt"
    val output = "/Users/leixinxin/Sites/output.txt"

    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile(input)

    import org.apache.flink.api.scala._

    val value: AggregateDataSet[(String, Int)] = text.flatMap(x => x.split(" "))
      .map(x => (x,1))
      .groupBy(0)
      .sum(1)

    value.writeAsText(output, FileSystem.WriteMode.OVERWRITE).setParallelism(1)

    env.execute("batch word count")
  }
}
