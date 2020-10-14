package Retail

import io.vertx.core.{Vertx, VertxOptions}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}


class MysqlDimensionAsyncFunction extends RichAsyncFunction {

  override def open(parameters: Configuration): Unit = {
    val vertx: Vertx = Vertx.vertx(
      new VertxOptions()
        .setWorkerPoolSize(10)
        .setEventLoopPoolSize(5)
    )
  }

  override def asyncInvoke(input: Nothing, resultFuture: ResultFuture[Nothing]): Unit = {

  }
}
