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
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.locks.ReentrantLock
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContextExecutor
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
        .childHandler(NettyTestInitializer())

      bootstrap.bind(8080).sync().channel().closeFuture().sync()
    } finally {
      eventLoopGroup.shutdownGracefully()
    }
  }
}

final class NettyTestInitializer extends ChannelInitializer[SocketChannel] {
  override def initChannel(channel: SocketChannel): Unit = {
    val pipeline = channel.pipeline()
    pipeline.addLast(new HttpResponseEncoder())
    pipeline.addLast(new HttpRequestDecoder(4096, 8192, 1))
    pipeline.addLast(NettyTestHandler(ExecutionContext.global))
  }
}

object NettyTestInitializer {
  def apply(): NettyTestInitializer = {
    new NettyTestInitializer()
  }
}

final class NettyTestHandler(
  implicit executor: ExecutionContextExecutor
) extends SimpleChannelInboundHandler[HttpObject] {
  var pending = Option.empty[DynamicInputStream]

  override def channelRead0(context: ChannelHandlerContext, message: HttpObject): Unit = {
    println(s"type of message: ${message.getClass().getName()}")

    message match {
      case request: HttpRequest =>
        // Got a new message start
        println("got a new message start")

      case lastContent: LastHttpContent =>
        println("got last message data")

        lastContent.retain

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
        println("got message data")

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
}

object NettyTestHandler {
  private val Content = Array[Byte]('H', 'e', 'l', 'l', 'o', ' ', 'W', 'o', 'r', 'l', 'd')

  def apply(implicit executor: ExecutionContextExecutor): NettyTestHandler = {
    new NettyTestHandler
  }

  private def copyInputStream(input: InputStream): Unit = {
    try {
      println("copying bytes...")
      Files.copy(input, Paths.get("/tmp/hello-world-netty"))
      println("did we copy or throw?")
    } catch {
      case NonFatal(e) =>
        e.printStackTrace()
    } finally {
      println("done with copy")
    }
  }
}

// TODO: deal with retain and release
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
}

