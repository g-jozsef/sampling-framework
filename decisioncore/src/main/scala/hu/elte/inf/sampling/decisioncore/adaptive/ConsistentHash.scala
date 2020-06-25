package hu.elte.inf.sampling.decisioncore.adaptive

import scala.util.hashing.MurmurHash3

class ConsistentHash(val weighting: Array[Double],
                     val replicationFactor: Int = 100,
                     val hashFunction: Any => Double = ConsistentHash.defaultHashFunction)
  extends Serializable {

  val numPartitions: Int = weighting.length
  private val numPackages = numPartitions * replicationFactor

  def getPartition(key: Any): Int = {
    hashRing(Math.floor(hashFunction(key) * numPackages).toInt % numPackages)
  }

  protected val hashRing: Array[Int] =
    ConsistentHash.createHashRing(weighting, replicationFactor)
}

object ConsistentHash {
  def createHashRing(weighting: Array[Double],
                     replicationFactor: Int): Array[Int] = {
    val numPackages: Int = weighting.length * replicationFactor
    val hashRing: Array[Int] = Array.fill[Int](numPackages)(0)

    var accumulatedWeight: Double = 0.0d
    var currentReplica: Int = 0
    var partition: Int = 0
    weighting.foreach {
      weight: Double =>
        accumulatedWeight += weight
        val lastReplica: Int = Math.rint(accumulatedWeight * numPackages).toInt
        while (currentReplica < lastReplica) {
          hashRing(currentReplica) = partition
          currentReplica += 1
        }
        partition += 1
    }

    hashRing
  }

  def defaultHashFunction: Any => Double = {
    key: Any => {
      (MurmurHash3.stringHash((key.hashCode + 123456791).toString).toDouble / Int.MaxValue + 1) / 2
    }
  }
}
