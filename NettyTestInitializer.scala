import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.http.HttpRequestDecoder
import io.netty.handler.codec.http.HttpResponseEncoder
import io.netty.handler.logging.LogLevel
import io.netty.handler.logging.LoggingHandler
import monix.execution.Scheduler

final class NettyTestInitializer(
  handler: ReactiveHandler
) extends ChannelInitializer[SocketChannel] {
  override def initChannel(channel: SocketChannel): Unit = {
    val pipeline = channel.pipeline()
    pipeline.addLast(new HttpResponseEncoder())
    pipeline.addLast(new HttpRequestDecoder(4096, 8192, 1))
    pipeline.addLast(new LoggingHandler(LogLevel.INFO))
    pipeline.addLast(NettyTestHandler(handler)(Scheduler.Implicits.global))
  }
}

object NettyTestInitializer {
  def apply(handler: ReactiveHandler): NettyTestInitializer = {
    new NettyTestInitializer(handler)
  }
}
