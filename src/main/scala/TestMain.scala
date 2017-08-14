import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.DefaultFileRegion
import io.netty.buffer.Unpooled
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.http.DefaultHttpHeaders
import io.netty.handler.codec.http.DefaultHttpResponse
import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.handler.codec.http.HttpHeaderValues
import io.netty.handler.codec.http.HttpMethod
import io.netty.handler.codec.http.HttpObject
import io.netty.handler.codec.http.HttpResponse
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.handler.codec.http.HttpVersion
import io.netty.handler.codec.http.LastHttpContent
import io.netty.handler.logging.LogLevel
import io.netty.handler.logging.LoggingHandler
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.control.NonFatal

object TestMain {
  private[this] val logger = LoggerFactory.getLogger(getClass())

  def main(args: Array[String]): Unit = {
    val eventLoopGroup = new NioEventLoopGroup()
    try {
      val bootstrap = new ServerBootstrap()
      bootstrap
        .group(eventLoopGroup)
        .channel(classOf[NioServerSocketChannel])
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(BridgeInitializer(HttpHandler(routes)))

      bootstrap.bind(8080).sync().channel().closeFuture().sync()
    } finally {
      eventLoopGroup.shutdownGracefully()
    }
  }

  private[this] val routes = List(
    Resource(
      _ == "/echo", {
        case HttpMethod.POST => echo
      }
    ),
    Resource(
      _ == "/big", {
        case HttpMethod.GET =>
          in =>
            Resource.consume(in).flatMap(_ => big)
      }
    ),
    Resource(
      _ == "/file", {
        case HttpMethod.GET =>
          in =>
            Resource
              .consume(in)
              .map { _ =>
                createResponseFromPath(
                  Paths.get("/home/jose/big_file")
                )
              }
      }
    )
  )

  private[this] val big: Task[(HttpResponse, Observable[Content])] = Task {
    val frames = 500000
    val size = 4096
    val buffer = Unpooled.unreleasableBuffer(
      Unpooled.wrappedBuffer(Array.fill(size)('a'.toByte))
    )

    val response = {
      val headers = new DefaultHttpHeaders()
      headers.set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN)
      headers.setInt(
        HttpHeaderNames.CONTENT_LENGTH,
        size * frames
      )

      new DefaultHttpResponse(
        HttpVersion.HTTP_1_1,
        HttpResponseStatus.OK,
        headers
      )
    }

    val body = Observable.range(0, frames).map(_ => Buffer(buffer))

    val lastBody = Observable.now(Http(LastHttpContent.EMPTY_LAST_CONTENT))

    (response, body ++ lastBody)
  }

  private def echo(
      in: Observable[HttpObject]
  ): Task[(HttpResponse, Observable[Content])] = {
    import Scheduler.Implicits.global

    val inputStream = new SubscriberInputStream()
    in.subscribe(inputStream)

    Task
      .fromFuture(copyInputStream(inputStream))
      .map(
        createInputStreamResponseFromPath
      )
  }

  private def copyInputStream(input: InputStream): Future[Path] = {
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

  private def createInputStreamResponseFromPath(
      path: Path
  ): (HttpResponse, Observable[Content]) = {
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
      val body = Observable.fromInputStream(Files.newInputStream(path)).map {
        bytes =>
          Buffer(Unpooled.wrappedBuffer(bytes))
      }

      val lastBody = Observable.now(Http(LastHttpContent.EMPTY_LAST_CONTENT))

      body ++ lastBody
    }

    (response, body)
  }

  private def createResponseFromPath(
      path: Path): (HttpResponse, Observable[Content]) = {
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

    val body = {
      val body = Observable.eval {
        Region(new DefaultFileRegion(path.toFile, 0, path.toFile.length()))
      }

      val lastBody = Observable.now(Http(LastHttpContent.EMPTY_LAST_CONTENT))

      body ++ lastBody
    }

    (response, body)
  }
}
