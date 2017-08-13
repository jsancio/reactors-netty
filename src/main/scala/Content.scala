import io.netty.buffer.ByteBuf
import io.netty.channel.FileRegion
import io.netty.handler.codec.http.HttpContent

sealed trait Content {
  def content: Object
}

final case class Http(val content: HttpContent) extends Content

final case class Buffer(val content: ByteBuf) extends Content

final case class Region(val content: FileRegion) extends Content
