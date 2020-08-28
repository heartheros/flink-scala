package DataSet

import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * @author Leixinxin
 * @date 2020/8/24 3:28 PM
 */
object FlinkDataSetPartition {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val sourceData: DataSet[(String)] = env.fromElements("aaa sss", "vbbb hello", "ok ??? hello", "flink kafka")
    val filterData: DataSet[(String)] = sourceData.filter(x => x.contains("hello"))
      .rebalance()

    filterData.print()
    env.execute()
  }
}
