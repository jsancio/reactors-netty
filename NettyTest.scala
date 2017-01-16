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
import io.reactors.Proto
import io.reactors.Reactor
import io.reactors.ReactorSystem
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Paths
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
      reply.events.onEvent {
        case request: HttpRequest =>
          logger.info(s"reactor($uid) - got a htttp request message: $request")
        case lastContent: LastHttpContent =>
          logger.info(s"reactor($uid) - got a last http content message: $lastContent")
          context.writeAndFlush(NettyTestHandler.createResponse())

          // TODO: Release now because we have nothing to do
          lastContent.release
        case content: HttpContent =>
          logger.info(s"reactor($uid) - got a http content message: $content")

          // TODO: Release now because we have nothing to do
          content.release
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

  /*
    message match {
      case request: HttpRequest =>

      case lastContent: LastHttpContent =>

        pending = pending match {
          case result @ Some(inputStream) =>
            inputStream.add(lastContent.content, true)
            result

          case None =>
            val inputStream = new DynamicInputStream(lastContent.content, true)

            Future(
              NettyTestHandler.copyInputStream(inputStream)
            ).foreach { _ =>
              val response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.OK,
                Unpooled.wrappedBuffer(NettyTestHandler.Content)
              )
              response.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN)
              response.headers().setInt(
                HttpHeaderNames.CONTENT_LENGTH,
                response.content().readableBytes()
              )

              context.channel.writeAndFlush(response)
              // TODO: we need to remove the pending object
            }

            Some(inputStream)
        }

      case content: HttpContent =>
        content.retain
        pending = pending match {
          case result @ Some(inputStream) =>
            inputStream.add(content.content, false)
            result

          case None =>
            val inputStream = new DynamicInputStream(content.content, false)

            // TODO: code duplication. remove
            Future(
              NettyTestHandler.copyInputStream(inputStream)
            ).foreach { _ =>
              val response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.OK,
                Unpooled.wrappedBuffer(NettyTestHandler.Content)
              )
              response.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN)
              response.headers().setInt(
                HttpHeaderNames.CONTENT_LENGTH,
                response.content().readableBytes()
              )

              context.channel.writeAndFlush(response)
              // TODO: we need to remove the pending object
            }

            Some(inputStream)
        }
    }
  }
*/
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

  private def copyInputStream(input: InputStream): Unit = {
    try {
      logger.info("copying bytes...")
      Files.copy(input, Paths.get("/tmp/hello-world-netty"))
      logger.info("we copied and didn't throw")
    } catch {
      case NonFatal(e) =>
        logger.error("why an exception", e)
    } finally {
      logger.info("done with copy")
    }
  }
}

final class DynamicInputStream(data: ByteBuf, done: Boolean) extends InputStream {
  private[this] val lock = new ReentrantLock()
  private[this] val notEmpty = lock.newCondition()

  private[this] var buffer = Vector(data)
  private[this] var isDone = done

  override def read(): Int = {
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

  def add(data: ByteBuf, done: Boolean): Unit = {
    lock.lock()
    try {
      isDone = done
      buffer = buffer :+ data
      notEmpty.signal()
    } finally {
      lock.unlock()
    }
  }

  // TODO: implement close - it should release all the buffer, clear the buffer and disable add
}
