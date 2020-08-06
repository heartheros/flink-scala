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
 * @date 2020/8/6 6:11 PM
 */
object FlinkWaterMarkWithLateness {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    import org.apache.flink.api.scala._
    val lineStream: DataStream[(String, Long)] = env.socketTextStream("localhost", 9000)
      .map(x => {
        var items: Array[String] = x.split(" ")
        (items(0), items(1).toLong)
      })

    val waterMarkStream: DataStream[(String, Long)] = lineStream.assignTimestampsAndWatermarks(
      new AssignerWithPeriodicWatermarks[(String, Long)] {
        var max: Long = 0L;
        var diff: Long = 10000L;
        var sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        override def getCurrentWatermark: Watermark = {
          val watermark: Watermark = new Watermark(max - diff)
          watermark
        }

        override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
          val eventTime: Long = element._2
          max = Math.max(max, eventTime)
          println(
            "currentThreadId:" + Thread.currentThread().getId
              + ",key:" + element._1
              + ",eventTime:[" + element._2 + "|" + sdf.format(element._2) + "]"
              + ",waterMarkTime:[" + this.getCurrentWatermark + "|" + sdf.format(this.getCurrentWatermark.getTimestamp) + "]"
          )
          element._2
        }
      }
    )

    waterMarkStream.keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .allowedLateness(Time.seconds(10))
      .apply(new MyWindowFunctionForLateness)
      .print()

    env.execute("with lateness")
  }
}

class MyWindowFunctionForLateness extends WindowFunction[(String, Long), String, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]): Unit = {
    val arrayBuffer = ArrayBuffer[Long]()
    val aa = input.iterator
    val keyStr = key.toString

    while (aa.hasNext) {
      val tup2 = aa.next()
      arrayBuffer.append(tup2._2)
    }

    val arr = arrayBuffer.toArray
    Sorting.quickSort(arr)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    out.collect(
      "window awake, key:" + keyStr
        + ",length:" + arr.length
        + ",first:" + sdf.format(arr.head)
        + ",last:" + sdf.format(arr.last)
        + ",windowStart:" + sdf.format(window.getStart)
        + ",windowEnd:" + sdf.format(window.getEnd)
    )
  }

}
