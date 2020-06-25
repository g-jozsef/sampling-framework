package hu.elte.inf.sampling.sampler.frequent

import hu.elte.inf.sampling.core.{AlgorithmConfiguration, HyperParameter, SamplingBase}

import scala.collection.mutable

class Frequent(config: AlgorithmConfiguration)
  extends SamplingBase(config) {

  private val topK: Int = config.getParam[Int]("topK")
  private val slidingWindowSize: Int = config.getParam[Int]("slidingWindowSize")
  private val basicWindowSize: Int = config.getParam[Int]("basicWindowSize")

  private val basicWindowCount: Int = slidingWindowSize / basicWindowSize

  private var basicRecordCount: Int = 0
  private var totalProcessedElements: Long = 0

  private val localCounter: mutable.Map[Any, Long] = mutable.Map.empty
  private val globalCounter: mutable.Map[Any, Long] = mutable.Map.empty
  private val globalQueue: mutable.Queue[Seq[(Any, Long)]] = mutable.Queue.empty // decr. ordered

  private var delta: Long = 0

  override def estimateMemoryUsage: Int = localCounter.size +
    globalCounter.size +
    globalQueue.map(item => item.length).sum

  override def getTotalProcessedElements: Long = totalProcessedElements

  override def recordKey(key: Any, multiplicity: Int): Unit = {
    var i = 0
    while (i < multiplicity) {
      // Update local counter
      if (localCounter.contains(key)) {
        localCounter(key) += 1
      } else {
        localCounter(key) = 1
      }
      // Upkeep
      basicRecordCount += 1
      totalProcessedElements += 1

      if (basicRecordCount == basicWindowSize) { // End of a basic window
        val newTopK: Seq[(Any, Long)] = localCounter.toSeq.sortBy(-_._2).take(topK)
        globalQueue.enqueue(newTopK)
        localCounter.clear()

        newTopK.foreach {
          case (data: Any, frequency: Long) =>
            if (globalCounter.contains(data)) {
              globalCounter(data) += frequency
            } else {
              globalCounter(data) = frequency
            }
        }

        delta += newTopK.last._2

        if (globalQueue.size > basicWindowCount) {
          val lastTopK: Seq[(Any, Long)] = globalQueue.dequeue()
          lastTopK.foreach {
            case (data: Any, frequency: Long) =>
              if (globalCounter(data) == frequency) {
                globalCounter.remove(data)
              } else {
                globalCounter(data) -= frequency
              }
          }
          delta -= lastTopK.last._2
        }

        basicRecordCount = 0
      }
      i += 1
    }
  }

  override def estimateProcessedKeyNumbers(): mutable.Map[Any, Long] = {
    globalCounter.filter(p => p._2 > delta)
  }
}

object Frequent {

  case class Config(topK: Int,
                    slidingWindowSize: Int,
                    basicWindowSize: Int)
    extends AlgorithmConfiguration("Frequent",
      "topK" -> new HyperParameter.TopK(topK),
      "slidingWindowSize" -> new HyperParameter.Window(slidingWindowSize),
      "basicWindowSize" -> new HyperParameter.Window(slidingWindowSize))

}