package hu.elte.inf.sampling.decisioncore.adaptive

import hu.elte.inf.sampling.core.PartitioningInfo

class ConsistentHashPartitioner(val weighting: Array[Double],
                                val replicationFactor: Int,
                                val hashFunction: Any => Double)
  extends Adaptive[ConsistentHashPartitioner] {

  override val numPartitions: Int = weighting.length
  protected val numPackages: Int = numPartitions * replicationFactor

  val consistentHash: ConsistentHash =
    new ConsistentHash(weighting, replicationFactor, hashFunction)

  override def adapt(partitioningInfo: PartitioningInfo,
                     newWeighting: Array[Double]): ConsistentHashPartitioner = {

    val newPartitioner: ConsistentHashPartitioner =
      new ConsistentHashPartitioner(newWeighting, replicationFactor, hashFunction)

    var accumulatedWeight: Double = 0.0d
    var oldAccumulatedWeight: Double = 0.0d
    var lastReplica: Int = 0
    var oldLastReplica: Int = 0
    var migrationCost: Int = 0
    var index: Int = 0

    while (index < Math.min(weighting.length, newWeighting.length)) {
      accumulatedWeight += newWeighting(index)
      oldAccumulatedWeight += weighting(index)

      val replicaCount =
        Math.rint(accumulatedWeight * numPackages).toInt - lastReplica
      val oldReplicaCount =
        Math.rint(oldAccumulatedWeight * numPackages).toInt - oldLastReplica

      if (oldReplicaCount < replicaCount) {
        migrationCost += replicaCount - oldReplicaCount
      }

      lastReplica = Math.rint(accumulatedWeight * numPackages).toInt
      oldLastReplica = Math.rint(oldAccumulatedWeight * numPackages).toInt
      index += 1
    }

    newPartitioner
  }

  override def getPartition(key: Any): Int = consistentHash.getPartition(key)
}
