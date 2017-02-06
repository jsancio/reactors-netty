import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.handler.codec.http.DefaultFullHttpResponse
import io.netty.handler.codec.http.DefaultHttpContent
import io.netty.handler.codec.http.DefaultHttpHeaders
import io.netty.handler.codec.http.DefaultHttpResponse
import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.handler.codec.http.HttpHeaderValues
import io.netty.handler.codec.http.HttpObject
import io.netty.handler.codec.http.HttpRequest
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
import scala.util.control.NonFatal


final class NettyTestHandler(
  handler: ReactiveHandler
)(
  implicit scheduler: Scheduler
) extends SimpleChannelInboundHandler[HttpObject] {
  private[this] val logger = LoggerFactory.getLogger(getClass())
  private[this] val observerKey = AttributeKey.valueOf[Observer[HttpObject]](getClass, "observer")

  override def channelActive(context: ChannelHandlerContext): Unit = {
    logger.info("handler - channel active")
  }

  override def channelInactive(context: ChannelHandlerContext): Unit = {
    logger.info("handler - channel inactive")
    // TODO: push on error
  }

  override def channelRead0(context: ChannelHandlerContext, message: HttpObject): Unit = {
    logger.info("handler - channel read")
    // Retain a reference before sending or returning message
    message match {
      case content: ReferenceCounted =>
        content.retain
      case _ =>
    }

    message match {
      case request: HttpRequest =>
        val subject = ConcurrentSubject.publishToOne[HttpObject]
        val old = context.channel.attr(observerKey).setIfAbsent(subject)

        if (old != subject) {
          // TODO: handler if the return value doesn't equal subject
        }

        // TODO: handler cancelable
        // TODO: handle all of the events
        handler(request)(subject).subscribe { reply =>
          context.writeAndFlush(reply)
          Ack.Continue
        }

        subject.onNext(request)
      case lastContent: LastHttpContent =>
        // TODOL What should we do if we don't have an observer?
        for (observer <- Option(context.channel.attr(observerKey).get)) {
          observer.onNext(lastContent)
          observer.onComplete()
        }
      case content: HttpObject =>
        // TODOL What should we do if we don't have an observer?
        for (observer <- Option(context.channel.attr(observerKey).get)) {
          observer.onNext(content)
        }
    }
  }
}

object NettyTestHandler {
  private val Content = Array[Byte]('H', 'e', 'l', 'l', 'o', ' ', 'W', 'o', 'r', 'l', 'd')
  private[this] val logger = LoggerFactory.getLogger(getClass())

  def apply(
    handler: ReactiveHandler
  )(
    implicit scheduler: Scheduler
  ): NettyTestHandler = {
    new NettyTestHandler(handler)
  }

  def createResponse: DefaultFullHttpResponse = {
    val response = new DefaultFullHttpResponse(
      HttpVersion.HTTP_1_1,
      HttpResponseStatus.OK,
      Unpooled.wrappedBuffer(Content)
    )
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN)
    response.headers().setInt(
      HttpHeaderNames.CONTENT_LENGTH,
      response.content().readableBytes()
    )

    response
  }

  def createResponseFromPath(path: Path): Observable[HttpObject] = {
    Observable.defer {
      val response = {
        val headers = new DefaultHttpHeaders()
        headers.set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN)
        headers.setInt(
          HttpHeaderNames.CONTENT_LENGTH,
          path.toFile.length().toInt
        )

        Observable.now(
          new DefaultHttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.OK,
            headers
          )
        )
      }

      val body = Observable.fromInputStream(Files.newInputStream(path)).map { bytes =>
        new DefaultHttpContent(Unpooled.wrappedBuffer(bytes))
      }

      val lastBody = Observable.now(LastHttpContent.EMPTY_LAST_CONTENT)

      response ++ body ++ lastBody
    }
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
