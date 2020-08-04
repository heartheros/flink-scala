import org.apache.flink.streaming.api.scala.{DataStream,StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @author Leixinxin
 * @date 2020/8/4 3:37 PM
 */
object Stream2Redis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val streamSource: DataStream[String] = env.fromElements("aaa bbb", "hello aaaa", "test aaa")

//    val tupleValue: DataStream[(String, String)] = env.fromElements("aaa bbb", "hello aaaa", "test aaa").map(
//      x => (x.split("_")(0), x.split("_")(1))
//    )

    val tupleValue: DataStream[(String, String)] = streamSource.map(
      x => (
        x.split(" ")(0),
        x.split(" ")(1)
      )
    )

    val builder = new FlinkJedisPoolConfig.Builder

    builder.setHost("127.0.0.1")
    builder.setPort(6379)

    builder.setTimeout(5000)
    builder.setMaxTotal(50)
    builder.setMaxTotal(10)
    builder.setMaxIdle(5)

    val config: FlinkJedisPoolConfig = builder.build()

    val redisSink = new RedisSink[Tuple2[String, String]](config, new MyRedisMapper)

    tupleValue.addSink(redisSink)

    env.execute()
  }

  class MyRedisMapper extends RedisMapper[Tuple2[String ,String]] {
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.SET)

    }

    override def getKeyFromData(t: (String, String)): String = {
      t._1
    }

    override def getValueFromData(t: (String, String)): String = {
      t._2
    }
  }
}
