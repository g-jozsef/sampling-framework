package hu.elte.inf.sampling.datagenerator

import java.io.{Closeable, FileOutputStream, FileWriter}
import java.nio.ByteBuffer

import hu.elte.inf.sampling.core.{Logger, Shop, StreamConfig}
import org.tukaani.xz.{LZMA2Options, XZOutputStream}

class DStreamWriter(driftConfig: StreamConfig, filePath: String = "") extends Logger {
  protected val options = new LZMA2Options(7)

  protected var outputStream: XZOutputStream = _
  protected var outputMeta: FileWriter = _
  protected var metaData: String = ""

  def init(): Unit = {
    outputStream = new XZOutputStream(new FileOutputStream(filePath + ".dstream"), options)
    outputMeta = new FileWriter(filePath + "_meta" + ".json")
    metaData = StreamConfigFactory.writePretty(driftConfig)
  }

  def writeStream(stream: Stream[Shop.Type]): Unit = {
    val preGeneratedStream = Array.fill(driftConfig.nElements)(stream.next())
    writeArray(preGeneratedStream)
  }

  def writeArray(vector: Array[Shop.Type]): Unit = {
    vector.foreach(item => writeObject(item))
    close(outputStream)

    outputMeta.write(metaData)
    close(outputMeta)
  }

  private def writeObject(shop: Shop.Type): Unit =
    outputStream.write(ByteBuffer.allocate(8).putLong(shop).array())

  private def close(stream: Closeable): Unit = {
    try {
      stream.close()
    } catch {
      case _: Throwable =>
        logError("Stream failed to close!")
    }
  }
}

object DStreamWriter {

  def write(driftConfig: StreamConfig, filePath: String, stream: Stream[Shop.Type]): Unit = {
    val writer: DStreamWriter = new DStreamWriter(driftConfig, filePath)
    writer.init()
    writer.writeStream(stream)
  }

  def write(driftConfig: StreamConfig, filePath: String, stream: Array[Shop.Type]): Unit = {
    val writer: DStreamWriter = new DStreamWriter(driftConfig, filePath)
    writer.init()
    writer.writeArray(stream)
  }
}