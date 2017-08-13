import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.handler.codec.http.HttpObject
import io.netty.handler.codec.http.HttpRequest
import io.netty.handler.codec.http.LastHttpContent
import io.netty.util.AttributeKey
import io.netty.util.ReferenceCounted
import monix.execution.Ack
import monix.execution.Scheduler
import monix.reactive.Observer
import monix.reactive.subjects.ConcurrentSubject
import org.slf4j.LoggerFactory
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success

final class BridgeHandler(
  handler: HttpHandler
)(
  implicit scheduler: Scheduler
) extends SimpleChannelInboundHandler[HttpObject] {
  private[this] val logger = LoggerFactory.getLogger(getClass())
  private[this] val observerKey = AttributeKey.valueOf[Observer[HttpObject]](getClass, "observer")
  private[this] val writableKey = AttributeKey.valueOf[Promise[Ack]](getClass, "writable")

  override def channelInactive(context: ChannelHandlerContext): Unit = {
    // TODO: Send error to observer if it exists
  }

  override def channelWritabilityChanged(context: ChannelHandlerContext): Unit = {
    if (context.channel.isWritable) {
      for (promise <- Option(context.channel.attr(writableKey).getAndSet(null))) {
        promise.success(Ack.Continue)
      }
    }
  }

  override def channelRead0(context: ChannelHandlerContext, message: HttpObject): Unit = {
    // Retain a reference before sending or returning message
    message match {
      case content: ReferenceCounted =>
        content.retain
      case _ =>
    }

    message match {
      case request: HttpRequest =>
        val subject = ConcurrentSubject.publishToOne[HttpObject]

        for (old <- Option(context.channel.attr(observerKey).setIfAbsent(subject))) {
          if (old != subject) {
            logger.warn("Client sent a request while we had one pending.")
            context.close()
            // TODO: old.onError(...)
          }
        }

        // TODO: handle cancelable
        handler(request)(subject).runOnComplete {
          case Success((response, body)) =>
            context.write(response)
            body.subscribe(BridgeObserver[Content](context, observerKey, writableKey))

          case Failure(error) =>
            // TODO: we should return a 500
            logger.error("Got an unhandle exception from the reactive handler", error)
            context.close()
        }
        subject.onNext(request)

      case lastContent: LastHttpContent =>
        // TODO: What should we do if we don't have an observer?
        for (observer <- Option(context.channel.attr(observerKey).get)) {
          observer.onNext(lastContent)
          observer.onComplete()
        }

      case content: HttpObject =>
        // TODO: What should we do if we don't have an observer?
        for (observer <- Option(context.channel.attr(observerKey).get)) {
          observer.onNext(content)
        }
    }
  }
}

object BridgeHandler {
  def apply(
    handler: HttpHandler
  )(
    implicit scheduler: Scheduler
  ): BridgeHandler = {
    new BridgeHandler(handler)
  }
}
