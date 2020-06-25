package hu.elte.inf.sampling.sampler

import hu.elte.inf.sampling.core.ErrorFunction.HellingerDistance
import hu.elte.inf.sampling.core.TypeDefs.TErrorPrecision
import hu.elte.inf.sampling.core._

import scala.collection.mutable
import scala.reflect.ClassTag

class CheckpointSmoothed[S <: Sampling : ClassTag, C <: AlgorithmConfiguration : ClassTag](config: AlgorithmConfiguration)
  extends SamplingBase(config) {
  private val testCase: SamplerCase[S, C] = config.getParam[SamplerCase[S, C]]("testCase")
  private val checkPointWindow: Int = config.getParam[Int]("checkPointWindow")
  private val triggerCheckTreshold: Int = config.getParam[Int]("triggerCheckTreshold")
  private val triggerErrorTreshold: Double = config.getParam[Double]("triggerErrorTreshold")

  private var innerSampling: Option[S] = Some(testCase.make)
  private var outerSampling: Option[S] = None

  private var totalProcessedElements: Long = 0
  private var lastCheckpoint: Long = 0

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

  def trigger(): Boolean = {
    outerSampling.isEmpty && totalProcessedElements > lastCheckpoint + checkPointWindow
  }

  def triggerPreCheck(): Boolean = {
    getOuterProcessedElements > triggerCheckTreshold
  }

  def triggerCheck(probabilityError: Double): Boolean = {
    probabilityError > triggerErrorTreshold
  }

  override def recordKey(key: Any, multiplicity: Int): Unit = {
    var i = 0
    while (i < multiplicity) {
      totalProcessedElements += 1
      innerSampling.foreach(x => x.recordKey(key))
      outerSampling.foreach(x => x.recordKey(key))

      if (trigger()) {
        outerSampling = Some(testCase.make)
      }

      if (triggerPreCheck()) {
        val hellinger = new HellingerDistance[Any]()
        val expected: Map[Any, TErrorPrecision] = innerSampling.map(x => x.estimateRelativeFrequencies()._2.toMap).get
        val actual: Map[Any, TErrorPrecision] = outerSampling.map(x => x.estimateRelativeFrequencies()._2.toMap).get

        if (actual.nonEmpty && expected.nonEmpty) {
          val result: TErrorPrecision = hellinger.hd(expected, actual)
          if (triggerCheck(result)) {
            innerSampling = outerSampling
          }
        }
        outerSampling = None
        lastCheckpoint = totalProcessedElements
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

object CheckpointSmoothed {
  def apply[S <: Sampling : ClassTag, C <: AlgorithmConfiguration : ClassTag](config: AlgorithmConfiguration): CheckpointSmoothed[S, C] =
    new CheckpointSmoothed[S, C](config)

  case class Config[+S <: Sampling : ClassTag, C <: AlgorithmConfiguration : ClassTag](testCase: SamplerCase[S, C],
                                                                                       checkPointWindow: Int,
                                                                                       triggerCheckTreshold: Int,
                                                                                       triggerErrorTreshold: Double)
    extends AlgorithmConfiguration(s"CPS\n${testCase.getConfig.shortName}",
      "testCase" -> new HyperParameter.EncapsulatedAlgorithm(testCase),
      "checkPointWindow" -> new HyperParameter.Window(checkPointWindow, 1000, 200000),
      "triggerCheckTreshold" -> new HyperParameter.Window(triggerCheckTreshold, 1000, 200000),
      "triggerErrorTreshold" -> new HyperParameter.DThreshold(triggerErrorTreshold))

}