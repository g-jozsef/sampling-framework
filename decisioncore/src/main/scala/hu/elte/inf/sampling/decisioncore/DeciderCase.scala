package hu.elte.inf.sampling.decisioncore

import hu.elte.inf.sampling.core.TestCase

import scala.reflect.ClassTag

trait DeciderCase[+S <: Decider[Any], +C] extends TestCase[S, C] {
  override def getConfig: C

  override def makeAlgorithm: S
}

object DeciderCase {
  def fromFactory[S <: Decider[Any] : ClassTag, C: ClassTag](config: C, factory: C => S): DeciderCase[S, C] = {
    new DeciderCase[S, C] {
      override def getConfig: C = config

      override def makeAlgorithm: S = factory(config)
    }
  }
}
