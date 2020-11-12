package DataSet

import org.apache.flink.api.scala.{ExecutionEnvironment,DataSet}
import org.apache.flink.configuration.Configuration
object BatchOperate {
  def main(args: Array[String]): Unit = {

    var inputPath = "/Users/luckychacha/Sites/words/"
    val configuration: Configuration = new Configuration()
    configuration.setBoolean("recursive.file.enumeration", true)


    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile(inputPath).withParameters(configuration)

    import org.apache.flink.api.scala._

    val res:DataSet[(String, Int)] = text.flatMap(x => x.split(" "))
      .map(x => (x, 1))
      .groupBy(0)
      .sum(1)

    res.print()
    env.execute("batch operate")
  }
}
