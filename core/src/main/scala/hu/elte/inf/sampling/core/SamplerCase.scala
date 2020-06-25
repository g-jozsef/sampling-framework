package hu.elte.inf.sampling.core

import scala.reflect.ClassTag

trait SamplerCase[+S <: Sampling, +C <: AlgorithmConfiguration] extends TestCase[S, C] {
  override def getConfig: C

  def make: S

  def getNumPartition: Int

  override def makeAlgorithm: S = make
}

object SamplerCase {
  def fromFactory[S <: Sampling : ClassTag, C <: AlgorithmConfiguration : ClassTag](config: C, factory: C => S, numPartition: Int = 10): SamplerCase[S, C] = {
    new SamplerCase[S, C] {
      override def getConfig: C = config

      override def make: S = factory(config)

      override def getNumPartition: Int = numPartition

      override def toString: String = getConfig.toString + "_" + "part=" + getNumPartition
    }
  }
}
