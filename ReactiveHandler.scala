import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.FileRegion
import io.netty.handler.codec.http.DefaultFullHttpResponse
import io.netty.handler.codec.http.DefaultHttpHeaders
import io.netty.handler.codec.http.DefaultHttpResponse
import io.netty.handler.codec.http.EmptyHttpHeaders
import io.netty.handler.codec.http.HttpContent
import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.handler.codec.http.HttpHeaderValues
import io.netty.handler.codec.http.HttpMethod
import io.netty.handler.codec.http.HttpObject
import io.netty.handler.codec.http.HttpRequest
import io.netty.handler.codec.http.HttpResponse
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.handler.codec.http.HttpVersion
import io.netty.handler.codec.http.LastHttpContent
import io.netty.util.ReferenceCounted
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable


trait Content {
  def content: Object
}
final case class Http(val content: HttpContent) extends Content
final case class Buffer(val content: ByteBuf) extends Content
final case class Region(val content: FileRegion) extends Content

final class ReactiveHandler private(
  resources: List[Resource]
) extends Function1[HttpRequest, Observable[HttpObject] => Task[(HttpResponse, Observable[Content])]] {
  override def apply(
    request: HttpRequest
  ): Observable[HttpObject] => Task[(HttpResponse, Observable[Content])] = {
    resources.find(resource => resource.pathMatch(request.uri)) match {
      case Some(resource) =>
        resource.handler.applyOrElse(
          request.method,
          (_: HttpMethod) =>
            in =>
              Resource.consume(in).map { _ =>
                (
                  Resource.createEmptyResponse(HttpResponseStatus.METHOD_NOT_ALLOWED),
                  Observable.empty
                )
              }
        )
      case None =>
        in =>
          Resource.consume(in).map { _ =>
            (
              Resource.createEmptyResponse(HttpResponseStatus.NOT_FOUND),
              Observable.empty
            )
          }
    }
  }
}

object ReactiveHandler {
  def apply(resources: List[Resource]): ReactiveHandler = {
    new ReactiveHandler(resources)
  }
}

case class Resource(
  pathMatch: String => Boolean,
  handler: PartialFunction[HttpMethod, Observable[HttpObject] => Task[(HttpResponse, Observable[Content])]]
)

object Resource {
  def echo(in: Observable[HttpObject]): Task[(HttpResponse, Observable[Content])] = {
    import Scheduler.Implicits.global

    Task.fromFuture(NettyTestHandler.copyInputStream(new DynamicInputStream(in))).map(
      NettyTestHandler.createResponseFromPath
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

  def createEmptyResponse(status: HttpResponseStatus): DefaultFullHttpResponse = {
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
}
