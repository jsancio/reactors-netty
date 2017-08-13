import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.http.HttpRequestDecoder
import io.netty.handler.codec.http.HttpResponseEncoder
import monix.execution.Scheduler

final class BridgeInitializer(
    handler: HttpHandler
) extends ChannelInitializer[SocketChannel] {
  override def initChannel(channel: SocketChannel): Unit = {
    val pipeline = channel.pipeline()
    pipeline.addLast(new HttpResponseEncoder())
    pipeline.addLast(new HttpRequestDecoder(4096, 8192, 1))
    // pipeline.addLast(new LoggingHandler(LogLevel.INFO))
    pipeline.addLast(BridgeHandler(handler)(Scheduler.Implicits.global))
  }
}

object BridgeInitializer {
  def apply(handler: HttpHandler): BridgeInitializer = {
    new BridgeInitializer(handler)
  }
}
