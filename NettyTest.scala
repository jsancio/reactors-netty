import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.http.HttpMethod
import io.netty.handler.logging.LogLevel
import io.netty.handler.logging.LoggingHandler
import monix.execution.Scheduler


object NettyTest {
  def main(args: Array[String]): Unit = {
    val eventLoopGroup = new NioEventLoopGroup()
    try {
      val bootstrap = new ServerBootstrap()
      bootstrap
        .group(eventLoopGroup)
        .channel(classOf[NioServerSocketChannel])
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(NettyTestInitializer(ReactiveHandler(routes)(Scheduler.Implicits.global)))

      bootstrap.bind(8080).sync().channel().closeFuture().sync()
    } finally {
      eventLoopGroup.shutdownGracefully()
    }
  }

  private[this] val routes = List(
    Resource(
      _ == "/echo"
      {
        case HttpMethod.POST => Resource.echo
      }
    )
  )
}
