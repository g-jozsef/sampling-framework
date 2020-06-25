package hu.elte.inf.sampling.sampler.lossycounting

import hu.elte.inf.sampling.core.{AlgorithmConfiguration, HyperParameter, SamplingBase}

import scala.collection.mutable

class LossyCounting(config: AlgorithmConfiguration)
  extends SamplingBase(config) {

  private val frequency: Double = config.getParam[Double]("frequency")
  private val error: Double = config.getParam[Double]("error")

  private val map = mutable.HashMap.empty[Any, LossyCounting.Data]

  def unsafeInnerState: mutable.HashMap[Any, LossyCounting.Data] = map

  private val windowSize = Math.ceil(1.0d / error)

  private var totalProcessedElements: Long = 0L
  private var currentBucket = 1

  /**
   * Memory usage in bytes
   */
  override def estimateMemoryUsage: Int = map.size * 2

  /**
   * Process a data entry with a given key
   */
  override def recordKey(key: Any, multiplicity: Int): Unit = {
    var i = 0
    while (i < multiplicity) {
      totalProcessedElements += 1

      if (map.contains(key)) {
        val d: LossyCounting.Data = map(key)
        map(key) = LossyCounting.Data(d.frequency + 1, d.error)
      } else {
        map(key) = LossyCounting.Data(1, currentBucket - 1)
      }

      if (totalProcessedElements % windowSize == 0) {
        map.keySet.foreach {
          key: Any =>
            val d: LossyCounting.Data = map(key)
            if (d.frequency + d.error <= currentBucket) {
              map.remove(key)
            }
        }

        currentBucket += 1
      }
      i += 1
    }
  }

  /**
   * Returns an estimate of the frequencies
   */
  override def estimateProcessedKeyNumbers(): mutable.Map[Any, Long] = {
    mutable.Map.empty[Any, Long] ++ map.keySet.filter {
      key: Any =>
        map(key).frequency.toDouble >=
          (frequency - error) * totalProcessedElements.toDouble
    }.map(key => (key, map(key).frequency))
  }

  /**
   * Over the lifetime of this sampler how much elements did it process
   */
  override def getTotalProcessedElements: Long = totalProcessedElements
}

object LossyCounting {

  case class Data(frequency: Long, error: Long)

  case class Config(frequency: Double = 0.0005,
                    error: Double = 0.00005)
    extends AlgorithmConfiguration("LossyCounting",
      "frequency" -> HyperParameter.Probability(frequency),
      "error" -> HyperParameter.Probability(error))

}
