import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.http.HttpMethod
import io.netty.handler.logging.LogLevel
import io.netty.handler.logging.LoggingHandler
import java.nio.file.Paths

object TestMain {
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
      _ == "/echo",
      {
        case HttpMethod.POST => Resource.echo
      }
    ),
    Resource(
      _ == "/big",
      {
        case HttpMethod.GET =>
          in => Resource.consume(in).flatMap(_ => Resource.big)
      }
    ),
    Resource(
      _ == "/file",
      {
        case HttpMethod.GET =>
          in =>
            Resource.consume(in).map(
              _ => Resource.createResponseFromPath(Paths.get("/home/jose/big_file"))
            )
      }
    )
  )
}
