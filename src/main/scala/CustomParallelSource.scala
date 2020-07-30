import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

/**
 * @author Leixinxin
 * @date 2020/7/29 4:17 PM
 */
class CustomParallelSource extends ParallelSourceFunction[String] {
  var isRunning:Boolean = true;
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    while(isRunning) {
      ctx.collect("hello world")
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
