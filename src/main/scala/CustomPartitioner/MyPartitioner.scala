package CustomPartitioner

import org.apache.flink.api.common.functions.Partitioner

/**
 * @author Leixinxin
 * @date 2020/8/4 2:30 PM
 */
class MyPartitioner extends Partitioner[String] {
  override def partition(key: String, numPartitions: Int): Int = {
    println("分区个数" + numPartitions)
    if (key.contains("hello")) {
      0
    } else {
      1
    }
  }
}
