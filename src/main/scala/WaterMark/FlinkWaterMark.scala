package WaterMark

import java.text.SimpleDateFormat

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting

/**
 * @author Leixinxin
 * @date 2020/8/5 4:47 PM
 */
object FlinkWaterMark {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val tupleStream: DataStream[(String, Long)] = env.socketTextStream("localhost", 9000).map(x => {
      val strings: Array[String] = x.split(" ")
      (strings(0), strings(1).toLong)
    })

    val waterMarkStream: DataStream[(String ,Long)] = {
      tupleStream.assignTimestampsAndWatermarks {
        new AssignerWithPeriodicWatermarks[(String ,Long)]{
          var currentTimeMillis:Long = 0L
          var timeDiff:Long = 10000L
          var sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

          override def getCurrentWatermark: Watermark = {
            val watermark: Watermark = new Watermark(currentTimeMillis - timeDiff)
            watermark
          }

          override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
            val eventTime = element._2
            currentTimeMillis = Math.max(eventTime, currentTimeMillis)
            val id = Thread.currentThread().getId

            println(
              "currentThreadId:" + id + ",key:" + element._1
              + ",eventTime:[" + element._2 + "|" + sdf.format(element._2) + "]"
              + ",currentMaxTimeStamp:[" + currentTimeMillis + "|" + sdf.format(currentTimeMillis) + "]"
              + ",waterMark:[" + this.getCurrentWatermark + "|" + sdf.format(this.getCurrentWatermark.getTimestamp) + "]"
            )

            eventTime
          }
        }
      }
    }

    waterMarkStream.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .apply(new MyWindowFunction)
      .print()

    env.execute()
  }
}

class MyWindowFunction extends WindowFunction[(String, Long), String, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]): Unit = {

    val keyStr = key.toString
    val arrBuff = ArrayBuffer[Long]()
    val ite = input.iterator

    while (ite.hasNext) {
      val tup2 = ite.next()
      arrBuff.append(tup2._2)
    }

    val arr = arrBuff.toArray
    Sorting.quickSort(arr)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    out.collect("聚合数据key为：" + keyStr
      + "，窗口中数据条数为" + arr.length
      + "，窗口中第一条数据：" + sdf.format(arr.head)
      + "，窗口中最后一条数据：" + sdf.format(arr.last)
      + "，窗口起始时间：" + sdf.format(window.getStart)
      + "，窗口结束时间：" + sdf.format(window.getEnd)

    )

  }
}
