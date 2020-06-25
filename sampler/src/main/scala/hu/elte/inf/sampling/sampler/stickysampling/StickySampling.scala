package hu.elte.inf.sampling.sampler.stickysampling

import java.util.concurrent.ThreadLocalRandom

import hu.elte.inf.sampling.core._

import scala.collection.mutable

class StickySampling(config: AlgorithmConfiguration)
  extends SamplingBase(config) {

  private val error: Double = config.getParam[Double]("error")
  private val frequency: Double = config.getParam[Double]("frequency")
  private val probabilityOfFailure: Double = config.getParam[Double]("probabilityOfFailure")


  private var totalProcessedElements = 0L
  private val map = mutable.HashMap.empty[Any, Long]

  def unsafeInnerState: mutable.HashMap[Any, Long] = map

  def getNextDouble: Double = {
    ThreadLocalRandom.current().nextDouble()
  }

  /**
   * The first t elements are sampled at rate r=1,
   * the next 2t are sampled at rate r=2,
   * the next 4t at r=4 and so on
   */
  private val t: Double =
    (1.0 / error) * Math.log(1.0 / (frequency * probabilityOfFailure))

  private var samplingRate: Int = 1
  private var rateStaysFor: Int = Math.rint(2 * t).toInt

  /**
   * Over the lifetime of this sampler how much elements did it process
   */
  override def getTotalProcessedElements: Long = totalProcessedElements

  /**
   * Memory usage in bytes
   */
  override def estimateMemoryUsage: Int = map.size

  /**
   * Process a data entry with a given key
   */
  override def recordKey(key: Any, multiplicity: Int): Unit = {
    var i = 0
    while (i < multiplicity) {
      totalProcessedElements += 1
      rateStaysFor -= 1

      if (map.contains(key)) {
        map(key) += 1
      } else {
        if (getNextDouble < (1.0 / samplingRate)) {
          map(key) = 1
        }
      }

      if (rateStaysFor == 0) {
        samplingRate *= 2
        rateStaysFor = Math.rint(samplingRate * t).toInt
        map.keySet.foreach {
          key: Any =>
            val frequency: Long = map(key)
            var lostCoinTosses: Long = 0
            while (lostCoinTosses < frequency && getNextDouble > 0.5) {
              lostCoinTosses += 1
            }

            if (frequency == lostCoinTosses) {
              map.remove(key)
            } else {
              map(key) -= lostCoinTosses
            }
        }
      }

      i += 1
    }
  }

  /**
   * Returns an estimate of the frequencies
   */
  override def estimateProcessedKeyNumbers(): mutable.Map[Any, Long] = {
    map.filter { itemWithFreq =>
      itemWithFreq._2.toDouble >= (frequency - error) * totalProcessedElements
    }
  }
}

object StickySampling {

  /**
   *
   * @param frequency above which we want to print out frequent items
   * @param error     output = f*N - e*N, where N is the total number of elements
   * @param probabilityOfFailure
   */
  case class Config(frequency: Double = 0.0005d,
                    error: Double = 0.00005d,
                    probabilityOfFailure: Double = 0.0001d)
    extends AlgorithmConfiguration("StickySampling",
      "frequency" -> HyperParameter.Probability(frequency),
      "error" -> HyperParameter.Probability(error),
      "probabilityOfFailure" -> HyperParameter.Probability(probabilityOfFailure))

}
