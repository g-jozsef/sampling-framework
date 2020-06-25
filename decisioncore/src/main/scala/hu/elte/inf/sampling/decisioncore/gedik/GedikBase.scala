package hu.elte.inf.sampling.decisioncore.gedik

import hu.elte.inf.sampling.decisioncore.adaptive.{ConsistentHash, ConsistentHashPartitioner}

import scala.collection.immutable.HashMap
import scala.collection.mutable

abstract class GedikBase(protected val numPartitions: Int,
                         protected val consistentHash: ConsistentHashPartitioner,
                         protected val currFrequencies: Map[Any, Double],
                         protected val prevFrequencies: Map[Any, Double],
                         protected val prevP: Any => Int,
                         protected val params: GedikPartitioner.Params) {

  def createHash(): Any => Int

  protected def lookup(explicitHash: mutable.Map[Any, Int],
                       consistentHash: ConsistentHash): Any => Int = {
    key: Any => {
      HashMap(explicitHash.toSeq: _*)
        .getOrElse(key, consistentHash.getPartition(key))
    }
  }

  protected def indicator(b: Boolean): Int = if (b) 1 else 0

  // Migration cost due to items not being tracked anymore
  @transient protected lazy val untrackedMigrationCost: Double = {
    val untrackedItems: Map[Any, Double] = prevFrequencies -- currFrequencies.keys
    untrackedItems.map {
      case (key: Any, frequency: Double) =>
        if (prevP(key) != consistentHash.consistentHash.getPartition(key)) frequency else 0.0d
    }.sum
  }

  @transient protected lazy val idealMigrationCost: Double = {
    val allItems: Map[Any, Double] = prevFrequencies ++ currFrequencies
    allItems.map {
      case (_: Any, frequency: Double) => params.betaS(frequency)
    }.sum / numPartitions
  }

  protected def memoryLoad(frequency: Double): Double =
    params.betaS(frequency)

  protected def computationLoad(frequency: Double): Double =
    frequency * params.betaC(frequency)

  protected def networkLoad(frequency: Double): Double =
    frequency

  protected def initialBalancePenalties: Seq[BalancePenalty] = {
    Seq(
      BalancePenalty(params.thetaS, memoryLoad, numPartitions),
      BalancePenalty(params.thetaC, computationLoad, numPartitions),
      BalancePenalty(params.thetaN, networkLoad, numPartitions)
    )
  }

  def balancePenaltyByExplicitHash(explicitHash: Map[Any, Int]): Double = {
    val memoryLoads: Array[Double] = Array.fill[Double](numPartitions)(0.0d)
    val computationLoads: Array[Double] = Array.fill[Double](numPartitions)(0.0d)
    val networkLoads: Array[Double] = Array.fill[Double](numPartitions)(0.0d)

    explicitHash.foreach {
      case (key: Any, partition: Int) =>
        val frequency = currFrequencies(key)
        memoryLoads(partition) += memoryLoad(frequency)
        computationLoads(partition) += computationLoad(frequency)
        networkLoads(partition) += networkLoad(frequency)
    }

    def resourceBalancePenalty(loads: Array[Double], theta: Double): Double = {
      val avgLoad = loads.sum / loads.length
      (loads.max - loads.min) / (theta * avgLoad)
    }

    Math.cbrt(
      resourceBalancePenalty(memoryLoads, params.thetaS) *
        resourceBalancePenalty(computationLoads, params.thetaC) *
        resourceBalancePenalty(networkLoads, params.thetaN)
    )
  }

}
