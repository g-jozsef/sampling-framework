package hu.elte.inf.sampling.datagenerator

import java.io.{Closeable, FileInputStream, FileReader}
import java.nio.ByteBuffer

import hu.elte.inf.sampling.core.{Logger, StreamConfig}
import hu.elte.inf.sampling.datagenerator.DStreamReader.ReaderFactory
import org.tukaani.xz.XZInputStream

class DStreamReader[T](filePath: String,
                       factory: ReaderFactory[T],
                       size: Int = 8) extends Stream[T] with Logger {
  protected val byteArray: Array[Byte] = new Array[Byte](size)

  protected var inputStream: XZInputStream = _

  override def init(): Unit =
    inputStream = new XZInputStream(new FileInputStream(filePath + ".dstream"))

  override def restart(): Unit = {
    close(inputStream)
    init()
  }

  def getMetadata: StreamConfig = {
    val reader = new FileReader(filePath + "_meta" + ".json")
    val driftConfig: StreamConfig = StreamConfigFactory(reader)
    close(reader)
    driftConfig
  }

  private def close(stream: Closeable): Unit = {
    try {
      stream.close()
    } catch {
      case _: Throwable => {
        logError("Stream failed to close!")
      }
    }
  }

  override def next(): T = {
    inputStream.read(byteArray)
    factory(ByteBuffer.wrap(byteArray))
  }

}

object DStreamReader {
  type ReaderFactory[T] = ByteBuffer => T
}