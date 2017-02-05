import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInitializer
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.http.DefaultFullHttpResponse
import io.netty.handler.codec.http.DefaultHttpContent
import io.netty.handler.codec.http.DefaultHttpHeaders
import io.netty.handler.codec.http.DefaultHttpResponse
import io.netty.handler.codec.http.HttpContent
import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.handler.codec.http.HttpHeaderValues
import io.netty.handler.codec.http.HttpHeaderValues
import io.netty.handler.codec.http.HttpObject
import io.netty.handler.codec.http.HttpRequest
import io.netty.handler.codec.http.HttpRequestDecoder
import io.netty.handler.codec.http.HttpResponseEncoder
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.handler.codec.http.HttpVersion
import io.netty.handler.codec.http.LastHttpContent
import io.netty.handler.logging.LogLevel
import io.netty.handler.logging.LoggingHandler
import io.netty.util.ReferenceCounted
import io.netty.util.AttributeKey
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.util.concurrent.locks.ReentrantLock
import monix.execution.Scheduler
import monix.execution.Ack
import monix.reactive.Observable
import monix.reactive.Observer
import monix.reactive.subjects.ConcurrentSubject
import org.slf4j.LoggerFactory
import scala.collection.immutable.Queue
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.control.NonFatal


object NettyTest {
  def main(args: Array[String]): Unit = {
    val eventLoopGroup = new NioEventLoopGroup()
    try {
      val bootstrap = new ServerBootstrap()
      bootstrap
        .group(eventLoopGroup)
        .channel(classOf[NioServerSocketChannel])
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(NettyTestInitializer(new ReactiveHandler()(Scheduler.Implicits.global)))

      bootstrap.bind(8080).sync().channel().closeFuture().sync()
    } finally {
      eventLoopGroup.shutdownGracefully()
    }
  }
}

final class ReactiveHandler(
  implicit scheduler: Scheduler
) extends (Observable[HttpObject] => Observable[HttpObject]) {
  override def apply(in: Observable[HttpObject]): Observable[HttpObject] = {
    Observable.fromFuture(
      NettyTestHandler.copyInputStream(new DynamicInputStream(in)).map(
        NettyTestHandler.createResponseFromPath
      )
    ).flatten
  }
}

final class NettyTestInitializer(
  handler: Observable[HttpObject] => Observable[HttpObject]
) extends ChannelInitializer[SocketChannel] {
  override def initChannel(channel: SocketChannel): Unit = {
    val pipeline = channel.pipeline()
    pipeline.addLast(new HttpResponseEncoder())
    pipeline.addLast(new HttpRequestDecoder(4096, 8192, 1))
    pipeline.addLast(new LoggingHandler(LogLevel.INFO))
    pipeline.addLast(NettyTestHandler(handler)(Scheduler.Implicits.global))
  }
}

object NettyTestInitializer {
  def apply(handler: Observable[HttpObject] => Observable[HttpObject]): NettyTestInitializer = {
    new NettyTestInitializer(handler)
  }
}



final class NettyTestHandler(
  handler: Observable[HttpObject] => Observable[HttpObject]
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
        handler(subject).subscribe { reply =>
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
    handler: Observable[HttpObject] => Observable[HttpObject]
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

final class DynamicInputStream(
  events: Observable[HttpObject]
)(
  implicit scheduler: Scheduler
) extends InputStream {
  private[this] val logger = LoggerFactory.getLogger(getClass())

  private[this] val lock = new ReentrantLock()
  private[this] val notEmpty = lock.newCondition()

  private[this] var buffer = Vector.empty[ByteBuf]
  private[this] var isDone = false

  events.subscribe { event =>
    event match {
      case data: HttpContent =>
        logger.info(s"reactor - got a http content message: $data")

        lock.lock()
        try {
          isDone = data.isInstanceOf[LastHttpContent]

          // We got the last content don't send any new events.
          if (isDone) {
            logger.info("reactor - closing subcription")
          }

          // Record the buffer in our cache
          buffer = buffer :+ data.content()
          notEmpty.signal()
        } finally {
          lock.unlock()
        }
      case _ => // Ignore everything else
    }

    Ack.Continue
  }

  override def read(): Int = {
    logger.info("any thread - read called")
    lock.lock()
    try {
      while (buffer.isEmpty && !isDone) {
        notEmpty.await()
      }

      if (buffer.nonEmpty && buffer(0).isReadable()) {
        val result = buffer(0).readByte() & 0xff

        if (!buffer(0).isReadable()) {
          buffer(0).release
          buffer = buffer.tail
        }

        result
      } else {
        -1
      }
    } finally {
      lock.unlock()
    }
  }

  override def close(): Unit = {
    // TODO: implement close - it should release all the buffer, clear the buffer and disable add
    logger.info("any thread - close called")
  }
}
