package hu.elte.inf.sampling.decisioncore.gedik

import hu.elte.inf.sampling.decisioncore.adaptive.ConsistentHashPartitioner
import hu.elte.inf.sampling.decisioncore.gedik.Readj.Adjustment

import scala.collection.mutable

class Readj(numPartitions: Int,
            consistentHash: ConsistentHashPartitioner,
            currFrequencies: Map[Any, Double],
            prevFrequencies: Map[Any, Double],
            prevP: Any => Int,
            params: GedikPartitioner.Params)
  extends GedikBase(numPartitions, consistentHash,
    currFrequencies, prevFrequencies, prevP, params) {

  def adjustBalancePenalty(balancePenalties: Seq[BalancePenalty],
                           adj: Adjustment): Seq[BalancePenalty] = {
    var adjustWeights: Seq[(Int, Double)] = Seq(
      adj.i -> -currFrequencies(adj.candidate1),
      adj.j -> currFrequencies(adj.candidate1)
    )

    if (adj.candidate2.nonEmpty) {
      adjustWeights = adjustWeights ++ Seq(
        adj.j -> -currFrequencies(adj.candidate2.get),
        adj.i -> currFrequencies(adj.candidate2.get)
      )
    }

    balancePenalties.map(_.withAddedWeights(adjustWeights))
  }

  def adjustMigrationPenalty(migrationCost: Double,
                             adj: Adjustment): Double = {
    var migrationPenalty: Double = 0.0d

    migrationPenalty +=
      params.betaS(currFrequencies(adj.candidate1)) * indicator(prevP(adj.candidate1) == adj.i) -
        params.betaS(currFrequencies(adj.candidate1)) * indicator(prevP(adj.candidate1) == adj.j)

    if (adj.candidate2.nonEmpty) {
      val frequency: Double = currFrequencies(adj.candidate2.get)
      migrationPenalty +=
        params.betaS(frequency) * indicator(prevP(adj.candidate2.get) == adj.j) -
          params.betaS(frequency) * indicator(prevP(adj.candidate2.get) == adj.i)
    }

    migrationPenalty += migrationCost
    migrationPenalty
  }

  override def createHash(): Any => Int = {
    var balancePenalties: Seq[BalancePenalty] = initialBalancePenalties

    var migrationCost: Double = untrackedMigrationCost

    val explicitHash: mutable.Map[Any, Int] = mutable.Map.empty
    val inverseExplicitHash: mutable.Map[Int, Set[Any]] = mutable.Map.empty

    // Tracked items stay put initially
    currFrequencies.foreach {
      case (key: Any, _: Double) =>
        val newPartition: Int = prevP(key)
        explicitHash(key) = newPartition
        inverseExplicitHash(newPartition) =
          if (inverseExplicitHash.contains(newPartition)) {
            inverseExplicitHash(newPartition) + key
          } else {
            Set(key)
          }
    }

    val weights: Seq[(Int, Double)] = explicitHash.map {
      case (key: Any, partition: Int) =>
        partition -> currFrequencies(key)
    }.toSeq

    // initialize balance penalties
    balancePenalties = balancePenalties.map(_.withAddedWeights(weights))

    var lastUtility: Double = 0.0d

    var improvementPossible: Boolean = true

    var bestReadjustment: Option[Adjustment] = None
    var bestGain: Double = Double.NegativeInfinity

    while (improvementPossible) {
      for {
        i: Int <- 0 until numPartitions
        j: Int <- 0 until numPartitions
        if i != j
      } {
        for {
          candidate1: Any <- inverseExplicitHash.getOrElse(i, Set())
          candidate2: Option[Any] <- inverseExplicitHash.getOrElse(j, Set()).map(Some(_)) ++ None
        } {
          val adjustedPenalty = Math.cbrt(
            adjustBalancePenalty(balancePenalties, Adjustment(i, candidate1, j, candidate2))
              .map(_.penalty).product)

          val prevPenalty =
            Math.cbrt(balancePenalties.map(_.penalty).product)

          if (adjustedPenalty < prevPenalty) {

            val migrationPenalty: Double =
              adjustMigrationPenalty(migrationCost, Adjustment(i, candidate1, j, candidate2)) /
                idealMigrationCost

            // Placement utility
            val currUtility = params.utility(adjustedPenalty, migrationPenalty)

            val currGain = (lastUtility - currUtility) /
              Math.abs(currFrequencies(candidate1) - candidate2.map(currFrequencies).getOrElse(0.0))

            if (currGain > bestGain) {
              bestReadjustment = Some(Adjustment(i, candidate1, j, candidate2))
              bestGain = currGain
              lastUtility = currUtility
            }
          }
        }
      }

      if (bestReadjustment.isEmpty) {
        improvementPossible = false
      } else {
        val adj: Adjustment = bestReadjustment.get

        explicitHash(adj.candidate1) = adj.j
        if (adj.candidate2.nonEmpty) {
          explicitHash(adj.candidate2.get) = adj.i
        }

        balancePenalties = adjustBalancePenalty(balancePenalties, adj)
        migrationCost = adjustMigrationPenalty(migrationCost, adj)

        inverseExplicitHash(adj.i) = inverseExplicitHash(adj.i) - adj.candidate1
        inverseExplicitHash(adj.j) = inverseExplicitHash(adj.j) + adj.candidate1

        if (adj.candidate2.nonEmpty) {
          inverseExplicitHash(adj.i) = inverseExplicitHash(adj.i) + adj.candidate2.get
          inverseExplicitHash(adj.j) = inverseExplicitHash(adj.j) - adj.candidate2.get
        }
      }
    }

    lookup(explicitHash, consistentHash.consistentHash)
  }
}

object Readj {

  case class Adjustment(i: Int, candidate1: Any, j: Int, candidate2: Option[Any])

}
