import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.http.HttpObject
import io.netty.util.AttributeKey
import monix.execution.Ack
import monix.reactive.Observer
import org.slf4j.LoggerFactory
import scala.concurrent.Future
import scala.concurrent.Promise

final class NettyBridgeObserver[T] private (
  context: ChannelHandlerContext,
  observerKey: AttributeKey[Observer[HttpObject]],
  writableKey: AttributeKey[Promise[Ack]]
) extends Observer[T] {
  private[this] val logger = LoggerFactory.getLogger(getClass())

  // TODO: implement this using a future based on the writtability
  override def onNext(elem: T): Future[Ack] = {
    context.write(elem, context.voidPromise)

    if (!context.channel.isWritable) {
      val promise = Promise[Ack]()
      // TODO: Log an error if it doesn't hold
      context.channel.attr(writableKey).setIfAbsent(promise)
      context.flush()
      promise.future
    } else {
      Ack.Continue
    }
  }

  override def onComplete(): Unit = {
    context.flush()
    context.channel.attr(observerKey).set(null)
  }

  override def onError(ex: Throwable): Unit = {
    logger.error("Unhandle exception from observer", ex)
    // TODO: we maybe able to do better. E.g. If we never sent a response we can send a 500
    context.close()
    context.channel.attr(observerKey).set(null)
  }
}

object NettyBridgeObserver {
  def apply[T](
    context: ChannelHandlerContext,
    observerKey: AttributeKey[Observer[HttpObject]],
    writableKey: AttributeKey[Promise[Ack]]
  ): NettyBridgeObserver[T] = {
    new NettyBridgeObserver[T](context, observerKey, writableKey)
  }
}
