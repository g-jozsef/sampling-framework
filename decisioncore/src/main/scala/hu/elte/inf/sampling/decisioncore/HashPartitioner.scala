package hu.elte.inf.sampling.decisioncore

import hu.elte.inf.sampling.core.Partitioner

class HashPartitioner(val numPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int = {
    val mod =
      if (key == null)
        0
      else
        key.hashCode() % numPartitions

    if (mod < 0)
      mod + numPartitions
    else
      mod
  }
}
