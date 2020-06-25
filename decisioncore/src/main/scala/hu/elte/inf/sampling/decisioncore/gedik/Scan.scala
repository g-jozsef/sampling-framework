package hu.elte.inf.sampling.decisioncore.gedik

import hu.elte.inf.sampling.decisioncore.adaptive.ConsistentHashPartitioner

import scala.collection.mutable

class Scan(numPartitions: Int,
           consistentHash: ConsistentHashPartitioner,
           currFrequencies: Map[Any, Double],
           prevFrequencies: Map[Any, Double],
           prevP: Any => Int,
           params: GedikPartitioner.Params)
  extends GedikBase(numPartitions, consistentHash,
    currFrequencies, prevFrequencies, prevP, params) {

  override def createHash(): Any => Int = {
    var balancePenalties: Seq[BalancePenalty] = initialBalancePenalties

    var migrationCost: Double = untrackedMigrationCost

    // The mapping is initially empty
    val explicitMapping: mutable.Map[Any, Int] = mutable.Map.empty

    // Items to place, in decr. freq. order
    val currFrequenciesSorted: Seq[(Any, Double)] = currFrequencies.toSeq.sortBy(-_._2)

    currFrequenciesSorted.foreach {
      case (key: Any, frequency: Double) =>
        var bestPlacement: Int = -1
        var bestUtility: Double = Double.PositiveInfinity
        val oldLocation: Int = prevP(key)

        // for each placement
        var partitionIndex = 0
        while (partitionIndex < numPartitions) {
          val balancePenalty = Math.cbrt(
            balancePenalties
              .map(_.withAddedWeights(Seq(partitionIndex -> frequency)).penalty).product
          )
          val migrationPenalty = (migrationCost +
            params.betaS(frequency) * indicator(partitionIndex != oldLocation)) / idealMigrationCost

          val utilityOfPlacement = params.utility(balancePenalty, migrationPenalty)

          // A better placement
          if (utilityOfPlacement < bestUtility) {
            bestPlacement = partitionIndex
            bestUtility = utilityOfPlacement
          }

          partitionIndex += 1
        }

        balancePenalties = balancePenalties.map(_.withAddedWeights(Seq(bestPlacement -> frequency)))
        migrationCost += params.betaS(frequency) * indicator(bestPlacement != oldLocation)

        explicitMapping(key) = bestPlacement
    }

    lookup(explicitMapping, consistentHash.consistentHash)
  }

}
