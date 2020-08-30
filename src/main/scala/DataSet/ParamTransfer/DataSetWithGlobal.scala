package DataSet.ParamTransfer

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

object DataSetWithGlobal {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val configuration: Configuration = new Configuration()
    configuration.setString("parameterKey", "aaa test")
    env.getConfig.setGlobalJobParameters(configuration)

    env.fromElements("aaa sdlasdjkaslkda", "asdasdas aaa test ???", "hello flink")
      .filter(new MyFilter)
      .print()
  }
}

class MyFilter extends RichFilterFunction[String] {
  var value:String = ""
  override def open(parameters: Configuration): Unit = {
    val parameters: ExecutionConfig.GlobalJobParameters = getRuntimeContext.getExecutionConfig.getGlobalJobParameters

    val globalConf: Configuration = parameters.asInstanceOf[Configuration]
    value = globalConf.getString("parameterKey", "default")
  }

  override def filter(t: String): Boolean = {
    if (t.contains(value)) true else false
  }
}
