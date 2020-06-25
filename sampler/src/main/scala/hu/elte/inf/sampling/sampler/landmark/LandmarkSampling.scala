package hu.elte.inf.sampling.sampler.landmark

import hu.elte.inf.sampling.core.{AlgorithmConfiguration, HyperParameter, SamplingBase}

import scala.collection.mutable

/**
 * Returns the true distribution of the stream each window
 *
 * Note: It can'Any be used on partitioned streams it only works with one partition!
 */
class LandmarkSampling(config: AlgorithmConfiguration)
  extends SamplingBase(config) {

  private val windowSize: Int = config.getParam[Int]("windowSize")

  protected var elements: Int = 0

  protected var map: mutable.HashMap[Any, Long] = mutable.HashMap.empty

  override def estimateMemoryUsage: Int = map.size

  override def getTotalProcessedElements: Long = elements

  override def recordKey(key: Any, multiplicity: Int): Unit = {
    var i = 0
    while (i < multiplicity) {
      if (elements % windowSize == 0)
        map.clear()

      elements = elements + 1

      map.get(key) match {
        case Some(value) =>
          map.put(key, value + 1)
        case None =>
          map.put(key, 1)
      }
      i += 1
    }
  }

  override def estimateProcessedKeyNumbers(): mutable.Map[Any, Long] = map
}

object LandmarkSampling {
  def apply(config: Config): LandmarkSampling = new LandmarkSampling(config)

  case class Config(windowSize: Int)
    extends AlgorithmConfiguration("LandmarkSampling",
      "windowSize" -> new HyperParameter.Window(windowSize))

}