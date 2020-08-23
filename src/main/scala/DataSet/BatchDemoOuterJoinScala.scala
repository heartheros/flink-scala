package DataSet

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

object BatchDemoOuterJoinScala {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val data1 = ListBuffer[Tuple2[Int, String]]()
    data1.append((1, "beijing"))
    data1.append((2, "guangdong"))
    data1.append((3, "fujian"))

    val data2 = ListBuffer[Tuple2[Int, String]]()
    data2.append((1, "beijing"))
    data2.append((2, "shenzhen"))
    data2.append((2, "dongguan"))
    data2.append((3, "fuzhou"))
    data2.append((3, "xiamen"))
    data2.append((4, "xianggang"))

    val text1 = env.fromCollection(data1)
    val text2 = env.fromCollection(data2)

    text1.leftOuterJoin(text2).where(0).equalTo(0).apply((left, right) => {
      if (right == null) {
        (left._1, left._2, null)
      } else {
        (left._1, left._2, right._2)
      }
    }).print()

    println("==========")

    text1.fullOuterJoin(text2).where(0).equalTo(0).apply((left, right) => {
      if (left == null) {
        (right._1, null, right._2)
      } else if (right == null) {
        (left._1, left._2, null)
      } else {
        (left._1, left._2, right._2)
      }
    }).print()

    env.execute("outer join")
  }

}
