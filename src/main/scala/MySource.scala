import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
 * @author Leixinxin
 * @date 2020/7/29 3:36 PM
 */
class MySource extends SourceFunction[String] {
  var isRunning: Boolean = true
  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
    while(isRunning) {
      sourceContext.collect("hello world")
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
