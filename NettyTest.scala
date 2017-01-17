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
import io.reactors.Channel
import io.reactors.Events
import io.reactors.Proto
import io.reactors.Reactor
import io.reactors.ReactorSystem
import io.reactors.Subscription
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.util.concurrent.locks.ReentrantLock
import org.slf4j.LoggerFactory
import scala.collection.immutable.Queue
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.Future
import scala.util.control.NonFatal


object NettyTest {
  def main(args: Array[String]): Unit = {
    val eventLoopGroup = new NioEventLoopGroup()
    try {
      val system = new ReactorSystem("test-system")

      val bootstrap = new ServerBootstrap()
      bootstrap
        .group(eventLoopGroup)
        .channel(classOf[NioServerSocketChannel])
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(NettyTestInitializer(system))

      bootstrap.bind(8080).sync().channel().closeFuture().sync()
    } finally {
      eventLoopGroup.shutdownGracefully()
    }
  }
}

sealed trait ChannelMessage
case class ActiveChannelMessage(context: ChannelHandlerContext) extends ChannelMessage
case object InactiveChannelMessage extends ChannelMessage

sealed trait ReactorNettyEvent
case class ReadChannel(channel: Channel[HttpObject]) extends ReactorNettyEvent

final class ReactorTest extends Reactor[ChannelMessage] {
  private[this] val logger = LoggerFactory.getLogger(getClass())
  private[this] var pending = Option.empty[DynamicInputStream]

  main.events.onEvent {
    case ActiveChannelMessage(context) =>
      logger.info(s"reactor($uid) - channel active")
      val reply = system.channels.daemon.open[HttpObject]
      context.pipeline().fireUserEventTriggered(ReadChannel(reply.channel))
      reply.events.onMatch {
        case request: HttpRequest =>
          implicit val executor = ExecutionContext.global
          logger.info(s"reactor($uid) - got a htttp request message: $request")

          for {
            path <- NettyTestHandler.copyInputStream(new DynamicInputStream(reply.events))
          } {
            context.writeAndFlush(NettyTestHandler.createResponse())
          }
      }

    case InactiveChannelMessage =>
      logger.info(s"reactor($uid) - channel inactive")
      main.seal()
  }
}

final class NettyTestInitializer(
  system: ReactorSystem
) extends ChannelInitializer[SocketChannel] {
  override def initChannel(channel: SocketChannel): Unit = {
    val pipeline = channel.pipeline()
    pipeline.addLast(new HttpResponseEncoder())
    pipeline.addLast(new HttpRequestDecoder(4096, 8192, 1))
    pipeline.addLast(NettyTestHandler(system)(ExecutionContext.global))
  }
}

object NettyTestInitializer {
  def apply(system: ReactorSystem): NettyTestInitializer = {
    new NettyTestInitializer(system)
  }
}

final class NettyTestHandler(
  system: ReactorSystem
)(
  implicit executor: ExecutionContextExecutor
) extends SimpleChannelInboundHandler[HttpObject] {
  private[this] val logger = LoggerFactory.getLogger(getClass())
  private[this] var channel = Option.empty[Channel[ChannelMessage]]
  private[this] var readChannel = Option.empty[Channel[HttpObject]]
  private[this] var buffer = Queue.empty[HttpObject]

  override def channelActive(context: ChannelHandlerContext): Unit = {
    logger.info("handler - channel active")
    val value = system.spawn(Proto[ReactorTest])
    channel = Some(value)
    value ! ActiveChannelMessage(context)
  }

  override def channelInactive(context: ChannelHandlerContext): Unit = {
    logger.info("handler - channel inactive")

    for (value <- channel) {
      value ! InactiveChannelMessage
    }
  }

  override def userEventTriggered(context: ChannelHandlerContext, event: AnyRef): Unit = {
    logger.info("handler - user event triggered")
    event match {
      case ReadChannel(value) =>
        readChannel = Some(value)
        for (message <- buffer) {
          value ! message
        }
        buffer = Queue.empty[HttpObject]
    }
  }

  override def channelRead0(context: ChannelHandlerContext, message: HttpObject): Unit = {
    logger.info("handler - channel read")
    // Retain a reference before sending or returning message
    message match {
      case content: ReferenceCounted =>
        content.retain
      case _ =>
    }

    readChannel match {
      case Some(value) =>
        // Send message
        value ! message
      case None =>
        buffer = buffer.enqueue(message)
    }
  }
}

object NettyTestHandler {
  private val Content = Array[Byte]('H', 'e', 'l', 'l', 'o', ' ', 'W', 'o', 'r', 'l', 'd')
  private[this] val logger = LoggerFactory.getLogger(getClass())

  def apply(
    system: ReactorSystem
  )(
    implicit executor: ExecutionContextExecutor
  ): NettyTestHandler = {
    new NettyTestHandler(system)
  }

  def createResponse(): DefaultFullHttpResponse = {
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

  def readInputStream(inputStream: InputStream, channel: Channel[Byte]): Future[Long] = {
    implicit val executor = ExecutionContext.global

    Future {
      var size = 0L
      var read = 0
      do {
        var read = inputStream.read()
        if (read != -1) {
          size += 1
          channel ! read.asInstanceOf[Byte]
        }
      } while (read != 1)

      size
    }
  }
}

final class DynamicInputStream(events: Events[HttpObject]) extends InputStream {
  private[this] val logger = LoggerFactory.getLogger(getClass())

  private[this] val lock = new ReentrantLock()
  private[this] val notEmpty = lock.newCondition()

  private[this] var buffer = Vector.empty[ByteBuf]
  private[this] var isDone = false

  // TODO: Deal with done
  private[this] val subcription: Subscription = events.onMatch {
    case data: HttpContent =>
      logger.info(s"reactor - got a http content message: $data")

      lock.lock()
      try {
        isDone = data.isInstanceOf[LastHttpContent]

        // We got the last content don't send any new events.
        if (isDone) {
          logger.info("reactor - closing subcription")
          subcription.unsubscribe
        }

        // Record the buffer in our cache
        buffer = buffer :+ data.content()
        notEmpty.signal()
      } finally {
        lock.unlock()
      }
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
