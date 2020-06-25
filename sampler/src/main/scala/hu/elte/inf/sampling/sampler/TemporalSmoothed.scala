package hu.elte.inf.sampling.sampler

import hu.elte.inf.sampling.core._

import scala.collection.mutable
import scala.reflect.ClassTag

class TemporalSmoothed[S <: Sampling : ClassTag, C <: AlgorithmConfiguration : ClassTag](config: AlgorithmConfiguration)
  extends SamplingBase(config) {
  val testCase: SamplerCase[S, C] = config.getParam[SamplerCase[S, C]]("testCase")
  val treshold: Int = config.getParam[Int]("treshold")
  val switchTreshold: Int = config.getParam[Int]("switchTreshold")

  private var innerSampling: Option[S] = Some(testCase.make)
  private var outerSampling: Option[S] = None

  private var totalProcessedElements: Long = 0

  def getInnerCounterCount: Int = {
    innerSampling.map(x => x.estimateMemoryUsage).getOrElse(0)
  }

  def getOuterCounterCount: Int = {
    outerSampling.map(x => x.estimateMemoryUsage).getOrElse(0)
  }

  def getTotalProcessedElements: Long = {
    totalProcessedElements
  }

  def getInnerProcessedElements: Long = {
    innerSampling.map(x => x.getTotalProcessedElements).getOrElse(0)
  }

  def getOuterProcessedElements: Long = {
    outerSampling.map(x => x.getTotalProcessedElements).getOrElse(0)
  }

  override def estimateMemoryUsage: Int = getInnerCounterCount + getOuterCounterCount

  override def recordKey(key: Any, multiplicity: Int): Unit = {
    var i = 0
    while (i < multiplicity) {
      totalProcessedElements += 1
      innerSampling.foreach(x => x.recordKey(key))
      outerSampling.foreach(x => x.recordKey(key))

      if (outerSampling.isEmpty && getInnerProcessedElements == treshold + switchTreshold) {
        outerSampling = Some(testCase.make)
      } else if (outerSampling.nonEmpty && getOuterProcessedElements == switchTreshold) {
        innerSampling = outerSampling
        outerSampling = None
      }
      i += 1
    }
  }

  override def estimateProcessedKeyNumbers(): mutable.Map[Any, Long] = {
    innerSampling.map(x => x.estimateProcessedKeyNumbers()).get
  }

  override def estimateRelativeFrequencies(): (Long, mutable.Map[Any, Double]) = {
    innerSampling.map(x => x.estimateRelativeFrequencies()).get
  }
}

object TemporalSmoothed {
  def apply[S <: Sampling : ClassTag, C <: AlgorithmConfiguration : ClassTag](config: AlgorithmConfiguration): TemporalSmoothed[S, C] =
    new TemporalSmoothed[S, C](config)

  case class Config[+S <: Sampling : ClassTag, C <: AlgorithmConfiguration : ClassTag](testCase: SamplerCase[S, C],
                                                                                       treshold: Int,
                                                                                       switchTreshold: Int)
    extends AlgorithmConfiguration(s"TMP\n${testCase.getConfig.shortName}",
      "testCase" -> new HyperParameter.EncapsulatedAlgorithm(testCase),
      "treshold" -> new HyperParameter.Threshold(treshold, 1000, 200000),
      "switchTreshold" -> new HyperParameter.Threshold(switchTreshold, 1000, 200000))

}