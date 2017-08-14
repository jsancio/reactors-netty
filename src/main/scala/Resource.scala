import io.netty.buffer.Unpooled
import io.netty.handler.codec.http.DefaultFullHttpResponse
import io.netty.handler.codec.http.DefaultHttpHeaders
import io.netty.handler.codec.http.EmptyHttpHeaders
import io.netty.handler.codec.http.HttpHeaderNames
import io.netty.handler.codec.http.HttpMethod
import io.netty.handler.codec.http.HttpObject
import io.netty.handler.codec.http.HttpResponse
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.handler.codec.http.HttpVersion
import io.netty.util.ReferenceCounted
import monix.eval.Task
import monix.reactive.Observable

case class Resource(
    pathMatch: String => Boolean,
    handler: PartialFunction[HttpMethod, BodyHandler]
)

object Resource {

  def consume(in: Observable[HttpObject]): Task[Unit] = {
    in.foreachL { message =>
      message match {
        case content: ReferenceCounted => content.release()
        case _ => // Not side effect or messages that are not ref counted
      }
    }
  }

  def createEmptyResponse(
      status: HttpResponseStatus,
      headers: Option[DefaultHttpHeaders]
  ): (HttpResponse, Observable[Content]) = {
    val effectiveHeader = headers.getOrElse(new DefaultHttpHeaders)
    effectiveHeader.setInt(HttpHeaderNames.CONTENT_LENGTH, 0)

    (
      new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1,
        status,
        Unpooled.EMPTY_BUFFER,
        effectiveHeader,
        EmptyHttpHeaders.INSTANCE
      ),
      Observable.empty
    )
  }
}
