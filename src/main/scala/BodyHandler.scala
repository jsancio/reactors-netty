import io.netty.handler.codec.http.HttpObject
import io.netty.handler.codec.http.HttpResponse
import monix.eval.Task
import monix.reactive.Observable


trait BodyHandler
extends Function[Observable[HttpObject], Task[(HttpResponse, Observable[Content])]]
