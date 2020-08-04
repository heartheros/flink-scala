package CustomPartitioner

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @author Leixinxin
 * @date 2020/8/4 2:34 PM
 */
object CustomPartitionerCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)
    import org.apache.flink.api.scala._
    val words: DataStream[String] = env.fromElements("aa", "hehe", "hello world", "hello flink", "hllo")

    val repartition: DataStream[String] = words.partitionCustom(new MyPartitioner, x => x + "")

    repartition.map(x => {
      println("数据的 key 为：" + x + "线程为：" + Thread.currentThread().getId)
      x
    })

    repartition.print()
    env.execute()
  }
}
