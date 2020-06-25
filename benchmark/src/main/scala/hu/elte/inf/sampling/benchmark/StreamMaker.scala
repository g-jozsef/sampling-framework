package hu.elte.inf.sampling.benchmark

import java.nio.file.{Files, Paths}

import hu.elte.inf.sampling.benchmark.Benchmarking.logDebug
import hu.elte.inf.sampling.benchmark.tools.InMemoryDataStream
import hu.elte.inf.sampling.core.{Logger, StreamConfig}
import hu.elte.inf.sampling.datagenerator.DStreamReader
import hu.elte.inf.sampling.datagenerator.DStreamReader.ReaderFactory
import org.json4s.{DefaultFormats, Formats, NoTypeHints}

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random

trait StreamMaker[T] {
  def id(): String

  def makeStream(): InMemoryDataStream[T]
}

object StreamMaker {

  val runTime: Long = 1e10.toLong
  val precision: Long = 1e6.toLong

  class StreamStreamMaker[T: ClassTag](nElements: Int,
                                       protected val stream: hu.elte.inf.sampling.datagenerator.Stream[T],
                                       windowSize: Int,
                                       topK: Int)
    extends StreamMaker[T] {

    override def id(): String = "" + topK.toString

    override def makeStream(): InMemoryDataStream[T] = {
      val windows: Array[Int] = Array.fill[Int](nElements / windowSize)(windowSize)
      val data: Array[T] = new Array[T](nElements)

      stream.restart()

      var index: Int = 0
      while (index < nElements) {
        data(index) = stream.next()
        index += 1
      }

      InMemoryDataStream(StreamConfig("aa", nElements, 10, "", null, null), null, windows, data)
    }
  }

  class FileStreamMaker[T: ClassTag](protected val filePath: String,
                                     windowSize: Int,
                                     topK: Int,
                                     readerFactory: ReaderFactory[T],
                                     oracleCalculator: OracleCalculator[T])
    extends StreamMaker[T] {

    override def id(): String = filePath + topK.toString

    override def makeStream(): InMemoryDataStream[T] = {
      val streamReader = new DStreamReader[T](filePath, readerFactory)

      val meta: StreamConfig = streamReader.getMetadata
      val oracle = oracleCalculator.calculateOracle(windowSize, topK, meta)
      val windows: Array[Int] = Array.fill[Int](meta.nElements / windowSize)(windowSize)
      val data: Array[T] = new Array[T](meta.nElements)

      streamReader.init()

      var index: Int = 0
      while (index < meta.nElements) {
        data(index) = streamReader.next()
        index += 1
      }

      InMemoryDataStream(meta, oracle, windows, data)
    }
  }

  class BustyFileStreamMaker[T: ClassTag](protected val filePath: String,
                                          windowSize: Int,
                                          topK: Int,
                                          readerFactory: ReaderFactory[T],
                                          oracleCalculator: OracleCalculator[T],
                                          config: BustyFileStreamMaker.Config)
    extends StreamMaker[T] with Logger {

    private val seed: Int = ((config.burstStartProbability * 100.0D).toInt
      + (config.keyBurstynessProbability * 100.0D).toInt * 1e2
      + config.burstLength._1 * 1e6
      + config.burstLength._2 * 1e8).toInt

    override def id(): String = "busty_" + filePath + topK.toString + seed.toString

    override def makeStream(): InMemoryDataStream[T] = {
      logInfo(s"Using BustyFileStreamMaker with parameters:\n" +
        s"windowSize = ${windowSize}\n" +
        s"burstStartProbabilityPerWindow = ${config.burstStartProbability}\n" +
        s"keyBurstynessProbability = ${config.keyBurstynessProbability}\n" +
        s"minimum burstWindowLength = ${config.burstLength._1}\n" +
        s"maximum burstWindowLength = ${config.burstLength._2}\n")
      val streamReader = new DStreamReader[T](filePath, readerFactory)

      val meta: StreamConfig = streamReader.getMetadata
      val oracle = oracleCalculator.calculateOracle(windowSize, topK, meta)
      val windows: Array[Int] = new Array[Int](meta.nElements / windowSize)
      val data: Array[T] = new Array[T](meta.nElements)

      streamReader.init()

      val windowCountIntention = meta.nElements / windowSize
      val burstHoldback: mutable.Map[T, Int] = mutable.Map.empty[T, Int]
      val random = new Random(seed)
      var dataIndex: Int = 0

      var currentBurst: Option[Int] = None
      var windowIndex: Int = 0
      while (windowIndex < windowCountIntention) {
        var thisWindowLength = 0
        currentBurst match {
          // collect bursted items and apply it to the generated elements
          case Some(burstDuration) if burstDuration == 0 =>
            burstHoldback.foreach {
              case (key: T, value: Int) =>
                var valueIndex = 0
                while (valueIndex < value) {
                  data(dataIndex) = key

                  dataIndex += 1
                  valueIndex += 1
                  thisWindowLength += 1
                }
            }
            assert(windowIndex * windowSize == dataIndex)

            burstHoldback.clear()
            currentBurst = None
          case Some(burstDuration) if burstDuration > 0 =>
            currentBurst = Some(burstDuration - 1)
          // Only start burst if we are not at the end already, and don't touch the first window
          case None if windowIndex > 0 && windowIndex + config.burstLength._2 < windowCountIntention =>
            if (random.nextDouble() < config.burstStartProbability) {
              // Start burst for random amount
              currentBurst = Some(random.nextInt(config.burstLength._2 - config.burstLength._1) + config.burstLength._1)
              oracle.relativeFreq(windowIndex).foreach {
                case freq: (T, Double) =>
                  if (random.nextDouble() < config.keyBurstynessProbability) {
                    burstHoldback.put(freq._1, 0)
                  }
              }
            }
          case _ =>
        }

        // Loop through the original idea of a windowsize
        // Put all elements that are not held back into the data array
        var i: Int = 0
        while (i < windowSize) {
          val nextElement: T = streamReader.next()
          burstHoldback.get(nextElement) match {
            // If value is held back, remember it
            case Some(oldValue) =>
              burstHoldback.put(nextElement, oldValue + 1)
            // If not, just allow it
            case None =>
              data(dataIndex) = nextElement

              dataIndex += 1
              thisWindowLength += 1
          }
          i += 1
        }
        windows(windowIndex) = thisWindowLength
        windowIndex += 1
      }

      InMemoryDataStream(meta, oracle, windows, data)
    }
  }

  object BustyFileStreamMaker {

    import org.json4s.native.Serialization

    /**
     * @param burstStartProbability    probability of a burst starting
     * @param keyBurstynessProbability probability of a single key beeing bursty
     * @param burstLength              the min and max length of a burst in microbatches
     */
    case class Config(burstStartProbability: Double = 0.3,
                      keyBurstynessProbability: Double = 0.2,
                      burstLength: (Int, Int) = (4, 10)) {
      def writeOut(path: String): Unit = {
        val dir = path + "bursty\\"
        Files.createDirectories(Paths.get(dir))
        val writer = Files.newBufferedWriter(Paths.get(dir + "burstconfig.json"))

        implicit val formats: Formats = DefaultFormats + NoTypeHints
        writer.write(Serialization.writePretty(this))
        writer.close()
      }
    }

  }

  class PooledStreamMaker[T: ClassTag](protected val streamMaker: StreamMaker[T]) extends StreamMaker[T] {
    override def id(): String = streamMaker.id()

    override def makeStream(): InMemoryDataStream[T] = {
      PooledStreamMaker.get(streamMaker)
    }
  }

  object PooledStreamMaker {
    val streamCache: mutable.Map[String, InMemoryDataStream[_]] = mutable.HashMap.empty

    def get[T: ClassTag](streamMaker: StreamMaker[T]): InMemoryDataStream[T] = synchronized {
      logDebug("getting " + streamMaker.id())
      if (streamCache.contains(streamMaker.id())) {
        logDebug("already have file " + streamMaker.id())
      } else {
        logDebug("getting file for the first time " + streamMaker.id())

        streamCache(streamMaker.id()) = streamMaker.makeStream()
      }

      streamCache(streamMaker.id()).asInstanceOf[InMemoryDataStream[T]]
    }
  }

}