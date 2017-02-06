import io.netty.buffer.ByteBuf
import io.netty.handler.codec.http.HttpContent
import io.netty.handler.codec.http.HttpObject
import io.netty.handler.codec.http.LastHttpContent
import java.io.InputStream
import java.util.concurrent.locks.ReentrantLock
import monix.execution.Ack
import monix.execution.Scheduler
import monix.reactive.Observable
import org.slf4j.LoggerFactory


final class DynamicInputStream(
  events: Observable[HttpObject]
)(
  implicit scheduler: Scheduler
) extends InputStream {
  private[this] val logger = LoggerFactory.getLogger(getClass())

  private[this] val lock = new ReentrantLock()
  private[this] val notEmpty = lock.newCondition()

  private[this] var buffer = Vector.empty[ByteBuf]
  private[this] var isDone = false

  events.subscribe { event =>
    event match {
      case data: HttpContent =>
        logger.info(s"reactor - got a http content message: $data")

        lock.lock()
        try {
          isDone = data.isInstanceOf[LastHttpContent]

          // We got the last content don't send any new events.
          if (isDone) {
            logger.info("reactor - closing subcription")
          }

          // Record the buffer in our cache
          buffer = buffer :+ data.content()
          notEmpty.signal()
        } finally {
          lock.unlock()
        }
      case _ => // Ignore everything else
    }

    Ack.Continue
  }

  override def read(): Int = {
    logger.info("any thread - read called")
    lock.lock()
    try {
      while (buffer.isEmpty && !isDone) {
        notEmpty.await()
      }

      if (buffer.nonEmpty && buffer(0).isReadable()) {
        val result = buffer(0).readByte() & 0xff

        if (!buffer(0).isReadable()) {
          buffer(0).release
          buffer = buffer.tail
        }

        result
      } else {
        -1
      }
    } finally {
      lock.unlock()
    }
  }

  override def close(): Unit = {
    // TODO: implement close - it should release all the buffer, clear the buffer and disable add
    logger.info("any thread - close called")
  }
}
