import io.netty.handler.codec.http.HttpMethod
import io.netty.handler.codec.http.HttpRequest
import io.netty.handler.codec.http.HttpResponseStatus
import monix.reactive.Observable

final class HttpHandler private (
    resources: List[Resource]
) extends Function1[HttpRequest, BodyHandler] {
  override def apply(
      request: HttpRequest
  ): BodyHandler = {
    resources.find(resource => resource.pathMatch(request.uri)) match {
      case Some(resource) =>
        resource.handler.applyOrElse(
          request.method,
          (_: HttpMethod) => { in =>
            Resource.consume(in).map { _ =>
              (
                Resource.createEmptyResponse(
                  HttpResponseStatus.METHOD_NOT_ALLOWED
                ),
                Observable.empty
              )
            }
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

object HttpHandler {
  def apply(resources: List[Resource]): HttpHandler = {
    new HttpHandler(resources)
  }
}
