package hu.elte.inf.sampling.sampler.oracle

import hu.elte.inf.sampling.core.StreamConfig.DriftConfig
import hu.elte.inf.sampling.core.{Distribution, StreamConfig}

import scala.collection.mutable
import scala.util.Random

/**
 * Returns the true distribution of the stream each window
 *
 * Note: It can't be used on partitioned streams it only works with one partition!
 */
class Oracle[T](config: Oracle.Config[T]) {
  assert(config.topK <= config.meta.nShops)

  val distributionMetas: Array[DriftConfig] =
    (Seq(DriftConfig(config.meta.initialDistribution, 0, 0)) ++ config.meta.drifters.map(x => x)).toArray

  val distributions: Array[Map[Int, Double]] =
    distributionMetas
      .map(
        x => new Random(x.distrConfig.seed)
          .shuffle((1 to config.meta.nShops).toList)
          .zip(Distribution(x.distrConfig.distr, config.meta.nShops, x.distrConfig.params).probabilities)
          .take(config.topK)
          .toMap
      )

  def computeOutput(nElements: Int): Array[(T, Long)] = {
    // Writing it this way in case drift stream generation changes to allow multiple drifts at once
    val runningDrifts: Array[Int] =
      distributionMetas.zipWithIndex.filter {
        case (drift: DriftConfig, _) =>
          nElements > drift.offsetPrc * config.meta.nElements - drift.driftWindow / 2 &&
            nElements < drift.offsetPrc * config.meta.nElements + drift.driftWindow / 2
      }.map(
        x => x._2
      )
    val frequencies: collection.Map[T, Double] =
      if (runningDrifts.isEmpty) {
        val distrIndex = distributionMetas.lastIndexWhere(
          x => nElements > x.offsetPrc * config.meta.nElements - x.driftWindow / 2
        )
        distributions(distrIndex)
          .map(
            x => (config.factory(x._1), x._2)
          )
      } else {
        // If drift stream generation changes this will catch it
        assert(runningDrifts.length == 1)
        val currentDriftIndex: Int =
          runningDrifts.head

        val currentDrift: DriftConfig =
          distributionMetas(currentDriftIndex)

        val prev: Map[Int, Double] =
          distributions(currentDriftIndex - 1)

        val curr: Map[Int, Double] =
          distributions(currentDriftIndex)


        val progressPercentage: Double =
          (nElements - (currentDrift.offsetPrc * config.meta.nElements - currentDrift.driftWindow / 2)) / currentDrift.driftWindow

        val resultMap: mutable.Map[T, Double] = mutable.Map.empty[T, Double]

        (prev.keySet ++ curr.keySet) foreach {
          key =>
            // Get values, assume zero if not in
            val prevValue: Double = prev.getOrElse(key, 0)
            val currValue: Double = curr.getOrElse(key, 0)

            resultMap.put(config.factory(key), currValue * progressPercentage + prevValue * (1 - progressPercentage))
        }

        resultMap
      }

    val smallest: Double = frequencies.last._2
    frequencies.toArray.sortBy(x => -x._2).take(config.topK).map(x => (x._1, (nElements.toDouble * x._2.toDouble / smallest).toLong))
  }
}

object Oracle {

  case class Config[T](windowSize: Int, meta: StreamConfig, topK: Int, factory: TFactory[T])

  type TFactory[T] = Int => T
}