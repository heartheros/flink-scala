package DataSet

import org.apache.flink.api.scala.ExecutionEnvironment

object DataSetSource {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val res:DataSet[(String, Int)] = env.fromCollection(Array("aaa bbb", "flink ok", "bska sdsajas", "test ??? aaa"))
      .flatMap(x => x.split(" "))
      .map(x => (x, 1))
      .groupBy(0)
      .sum(1)

    res.setParallelism(1).print()
    res.writeAsText("/Users/leixinxin/Sites/words/output")
    env.execute("data set collection source")

  }
}
