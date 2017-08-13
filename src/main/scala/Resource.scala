import io.netty.buffer.Unpooled
import io.netty.channel.DefaultFileRegion
import io.netty.handler.codec.http.DefaultFullHttpResponse
import io.netty.handler.codec.http.DefaultHttpHeaders
import io.netty.handler.codec.http.DefaultHttpResponse
import io.netty.handler.codec.http.EmptyHttpHeaders
import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.handler.codec.http.HttpHeaderValues
import io.netty.handler.codec.http.HttpMethod
import io.netty.handler.codec.http.HttpObject
import io.netty.handler.codec.http.HttpResponse
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.handler.codec.http.HttpVersion
import io.netty.handler.codec.http.LastHttpContent
import io.netty.util.ReferenceCounted
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.control.NonFatal

case class Resource(
    pathMatch: String => Boolean,
    handler: PartialFunction[HttpMethod, BodyHandler]
)

// TODO: Clean this up
object Resource {
  private[this] val logger = LoggerFactory.getLogger(getClass())

  def echo(
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

  val big: Task[(HttpResponse, Observable[Content])] = Task {
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

  def consume(in: Observable[HttpObject]): Task[Unit] = {
    in.foreachL { message =>
      message match {
        case content: ReferenceCounted => content.release()
        case _ => // Not side effect or messages that are not ref counted
      }
    }
  }

  def createEmptyResponse(
      status: HttpResponseStatus
  ): DefaultFullHttpResponse = {
    val headers = new DefaultHttpHeaders()
    headers.setInt(HttpHeaderNames.CONTENT_LENGTH, 0)

    new DefaultFullHttpResponse(
      HttpVersion.HTTP_1_1,
      status,
      Unpooled.EMPTY_BUFFER,
      headers,
      EmptyHttpHeaders.INSTANCE
    )
  }

  def createInputStreamResponseFromPath(
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

    val body = {
      val body = Observable.eval {
        Region(new DefaultFileRegion(path.toFile, 0, path.toFile.length()))
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
