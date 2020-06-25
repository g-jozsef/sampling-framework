package hu.elte.inf.sampling.decisioncore.gedik

import hu.elte.inf.sampling.decisioncore.adaptive.ConsistentHashPartitioner

import scala.collection.mutable

class Redist(numPartitions: Int,
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

    val toPlace: mutable.Set[(Any, Double)] = mutable.Set.empty

    currFrequencies.foreach(toPlace.add)

    while (toPlace.nonEmpty) {
      var bestPlacement: Int = -1
      var bestUtility: Double = Double.MaxValue
      var bestItem: Option[(Any, Double)] = None

      toPlace.foreach {
        case (key: Any, frequency: Double) =>
          val oldLocation: Int = prevP(key)

          // for each placement
          var partitionIndex = 0
          while (partitionIndex < numPartitions) {
            val balancePenalty = Math.cbrt(
              balancePenalties
                .map(_.withAddedWeights(Seq(partitionIndex -> frequency)).penalty).product
            )
            val migrationPenalty = (migrationCost +
              params.betaS(frequency) * indicator(partitionIndex != oldLocation)
              ) / idealMigrationCost

            val utilityOfPlacement = params.utility(balancePenalty, migrationPenalty) / frequency

            if (utilityOfPlacement < bestUtility) {
              bestPlacement = partitionIndex
              bestUtility = utilityOfPlacement
              bestItem = Some((key, frequency))
            }

            partitionIndex += 1
          }
      }

      balancePenalties =
        balancePenalties.map(_.withAddedWeights(Seq(bestPlacement -> bestItem.get._2)))

      migrationCost += params
        .betaS(bestItem.get._2) * indicator(bestPlacement != prevP(bestItem.get._1))

      explicitMapping(bestItem.get._1) = bestPlacement

      toPlace.remove(bestItem.get)
    }

    lookup(explicitMapping, consistentHash.consistentHash)
  }

}
