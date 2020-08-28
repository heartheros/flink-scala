package DataSet.ParamTransfer

import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
 * @author Leixinxin
 * @date 2020/8/24 6:18 PM
 */
object DataSetWithParams {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._


    val input: DataSet[(String)] = env.fromElements("aa ???", "hello aaa", "test bbb")

    val configuration = new Configuration()
    configuration.setString("FilterItem", "aa")

    val filterRes: DataSet[(String)] = input.filter(new MyParamsFilter()).withParameters(configuration)
    filterRes.print()

    env.execute("params transfer with params")
  }
}

class MyParamsFilter extends RichFilterFunction[String]() {
  var value: String = "";

  override def open(parameters: Configuration): Unit = {
    value = parameters.getString("FilterItem", "defaultValue")
  }

  override def filter(input: String): Boolean = {
    input.contains(value)
  }
}
