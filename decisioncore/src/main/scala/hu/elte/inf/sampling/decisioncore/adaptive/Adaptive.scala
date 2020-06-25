package hu.elte.inf.sampling.decisioncore.adaptive

import hu.elte.inf.sampling.core.{Partitioner, PartitioningInfo}

trait Adaptive[P <: Adaptive[P]] extends Partitioner {
  def adapt(partitioningInfo: PartitioningInfo, newWeighting: Array[Double]): P
}
