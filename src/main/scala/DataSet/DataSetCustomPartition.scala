package DataSet

import akka.stream.scaladsl.Partition
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * @author Leixinxin
 * @date 2020/8/24 3:44 PM
 */
object DataSetCustomPartition {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    import org.apache.flink.api.scala._

    val result: DataSet[String] = env.fromElements("aaa ????", "hello world", "world peace", "stay calm", "hello", "hello moto")
      .partitionCustom(new MyDataSetPartition, x => x+"")

    val value: DataSet[String] = result.map(x => {
      println("数据key为 " + x + " 线程为" + Thread.currentThread().getId)
      x
    })

    value.print()
    env.execute("custom partition")
  }
}

class MyDataSetPartition extends Partitioner[String] {
  override def partition(key: String, numPartitions: Int): Int = {
    if (key.contains("hello")) {
      println("0号分区")
      return 0
    }
    println("1号分区")
    1
  }
}
