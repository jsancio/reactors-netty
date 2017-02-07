import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.handler.codec.http.DefaultHttpHeaders
import io.netty.handler.codec.http.DefaultHttpResponse
import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.handler.codec.http.HttpHeaderValues
import io.netty.handler.codec.http.HttpObject
import io.netty.handler.codec.http.HttpRequest
import io.netty.handler.codec.http.HttpResponse
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.handler.codec.http.HttpVersion
import io.netty.handler.codec.http.LastHttpContent
import io.netty.util.AttributeKey
import io.netty.util.ReferenceCounted
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import monix.execution.Ack
import monix.execution.Scheduler
import monix.reactive.Observable
import monix.reactive.Observer
import monix.reactive.subjects.ConcurrentSubject
import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.control.NonFatal
import scala.util.Success
import scala.util.Failure


final class NettyTestHandler(
  handler: ReactiveHandler
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
            body.subscribe(NettyBridgeObserver[Content](context, observerKey, writableKey))

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

object NettyTestHandler {
  private[this] val logger = LoggerFactory.getLogger(getClass())

  def apply(
    handler: ReactiveHandler
  )(
    implicit scheduler: Scheduler
  ): NettyTestHandler = {
    new NettyTestHandler(handler)
  }

  def createResponseFromPath(path: Path): (HttpResponse, Observable[Content]) = {
    val response = {
      val headers = new DefaultHttpHeaders()
      headers.set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN)
      headers.setInt(
        HttpHeaderNames.CONTENT_LENGTH,
        path.toFile.length().toInt
      )

      new DefaultHttpResponse(
        HttpVersion.HTTP_1_1,
        HttpResponseStatus.OK,
        headers
      )
    }

    val body = Observable.defer {
      val body = Observable.fromInputStream(Files.newInputStream(path)).map { bytes =>
        Buffer(Unpooled.wrappedBuffer(bytes))
      }

      val lastBody = Observable.now(Http(LastHttpContent.EMPTY_LAST_CONTENT))

      body ++ lastBody
    }

    (response, body)
  }

  def copyInputStream(input: InputStream): Future[Path] = {
    implicit val executor = ExecutionContext.global

    Future {
      try {
        logger.info("copying bytes...")
        val path = Files.createTempFile("reactors", "test")
        Files.copy(input, path, StandardCopyOption.REPLACE_EXISTING)
        logger.info("we copied and didn't throw")
        path
      } catch {
        case NonFatal(e) =>
          logger.error("why an exception", e)
          throw e
      } finally {
        logger.info("done with copy")
      }
    }
  }
}
