package DataSet

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
 * @author Leixinxin
 * @date 2020/9/2 11:08 AM
 */
object DistributedFile {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    env.registerCachedFile("/Users/leixinxin/Downloads/a.log", "testFile")
    val testElements: DataSet[String] = env.fromElements("aaa", "bbb", "cccc", "hello")

    val res = testElements.map(new RichMapFunction[String, String] {
      override def open(parameters: Configuration): Unit = {
        val myFile = getRuntimeContext.getDistributedCache.getFile("testFile")
        val lines = FileUtils.readLines(myFile)
        val it = lines.iterator()
        while (it.hasNext) {
          println(it.next())
        }
      }
      override def map(value: String): String = {
        value
      }
    })
      .setParallelism(2)

    res.print()
    env.execute("distributed file")
  }

}
