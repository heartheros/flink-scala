package DataSet.ParamTransfer

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * @author Leixinxin
 * @date 2020/8/24 5:06 PM
 */
object DataSetConstruct {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val input: DataSet[(String)] = env.fromElements("aaa bbb", "ccc ???", "test", "test output")

    input.filter(new MyConstructFilter("test")).print()

    env.execute("construct param transfer")
  }
}

class MyConstructFilter(str: String) extends FilterFunction[String]{
  override def filter(value: String): Boolean = {
    value.contains(str)
  }
}
