package DataSet

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

import scala.collection.mutable
import java.util

object FlinkDataSetBroadCast {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val productData: DataSet[String] = env.readTextFile("/Users/luckychacha/Downloads/product.txt")

    val productDataMap = new mutable.HashMap[String, String]

    val productDataMapSet: DataSet[mutable.HashMap[String, String]] = productData.map(x => {
      productDataMap.put(x.split(",")(0), x)
      productDataMap
    })

    val orderData: DataSet[String] = env.readTextFile("/Users/luckychacha/Downloads/orders.txt")
    val res: DataSet[String] = orderData.map(new RichMapFunction[String, String] {
      var dataList: util.List[Map[String, String]] = null
      var allMap = Map[String, String]()
      override def open(parameters: Configuration): Unit = {
        dataList = getRuntimeContext.getBroadcastVariable[Map[String, String]]("ProductDataMapSet")
        val listIterator: util.Iterator[Map[String, String]] = dataList.iterator()
        while(listIterator.hasNext) {
          allMap = allMap.++(listIterator.next())
        }
      }
      override def map(order: String): String = {
        order + allMap.getOrElse(order.split(",")(2), "暂时没有匹配的值")
      }
    }).withBroadcastSet(productDataMapSet, "ProductDataMapSet")

    res.print()
    env.execute("broadcast params")
  }
}
