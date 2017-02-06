import io.netty.buffer.Unpooled
import io.netty.handler.codec.http.DefaultFullHttpResponse
import io.netty.handler.codec.http.DefaultHttpHeaders
import io.netty.handler.codec.http.EmptyHttpHeaders
import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.handler.codec.http.HttpMethod
import io.netty.handler.codec.http.HttpObject
import io.netty.handler.codec.http.HttpRequest
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.handler.codec.http.HttpVersion
import io.netty.util.ReferenceCounted
import monix.execution.Scheduler
import monix.reactive.Observable


final class ReactiveHandler private(
  resources: List[Resource]
)(
  implicit scheduler: Scheduler
) extends Function1[HttpRequest, (Observable[HttpObject] => Observable[HttpObject])] {
  override def apply(request: HttpRequest): Observable[HttpObject] => Observable[HttpObject] = {
    resources.find(resource => resource.pathMatch(request.uri)) match {
      case Some(resource) =>
        resource.handler.applyOrElse(
          request.method,
          (_: HttpMethod) => Resource.methodNotAllowed
        )
      case None =>
        Resource.notFound
    }
  }
}

object ReactiveHandler {
  def apply(resources: List[Resource])(implicit scheduler: Scheduler): ReactiveHandler = {
    new ReactiveHandler(resources)(scheduler)
  }
}

case class Resource(
  pathMatch: String => Boolean,
  handler: PartialFunction[HttpMethod, Observable[HttpObject] => Observable[HttpObject]]
)

object Resource {
  def echo(in: Observable[HttpObject]): Observable[HttpObject] = {
    import Scheduler.Implicits.global

    Observable.fromFuture(
      NettyTestHandler.copyInputStream(new DynamicInputStream(in)).map(
        NettyTestHandler.createResponseFromPath
      )
    ).flatten
  }

  def notFound(
    in: Observable[HttpObject]
  )(
    implicit scheduler: Scheduler
  ): Observable[HttpObject] = {
    Observable.fromTask {
      in.foldLeftL(createNotFoundResponse) { (state, current) =>
        current match {
          case content: ReferenceCounted => content.release()
          case _ => // Not side effect or messages that are not ref counted
        }

        state
      }
    }
  }

  def methodNotAllowed(
    in: Observable[HttpObject]
  )(
    implicit scheduler: Scheduler
  ): Observable[HttpObject] = {
    // TODO: Refactor this code duplication
    Observable.fromTask {
      in.foldLeftL(createMethodNotAllowedResponse) { (state, current) =>
        current match {
          case content: ReferenceCounted => content.release()
          case _ => // Not side effect or messages that are not ref counted
        }

        state
      }
    }
  }

  private[this] def createMethodNotAllowedResponse: DefaultFullHttpResponse = {
    val headers = new DefaultHttpHeaders()
    headers.setInt(HttpHeaderNames.CONTENT_LENGTH, 0)

    new DefaultFullHttpResponse(
      HttpVersion.HTTP_1_1,
      HttpResponseStatus.METHOD_NOT_ALLOWED,
      Unpooled.EMPTY_BUFFER,
      headers,
      EmptyHttpHeaders.INSTANCE
    )
  }

  private[this] def createNotFoundResponse: DefaultFullHttpResponse = {
    val headers = new DefaultHttpHeaders()
    headers.setInt(HttpHeaderNames.CONTENT_LENGTH, 0)

    new DefaultFullHttpResponse(
      HttpVersion.HTTP_1_1,
      HttpResponseStatus.NOT_FOUND,
      Unpooled.EMPTY_BUFFER,
      headers,
      EmptyHttpHeaders.INSTANCE
    )
  }
}
