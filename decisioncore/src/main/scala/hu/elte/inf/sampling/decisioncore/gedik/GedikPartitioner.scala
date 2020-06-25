package hu.elte.inf.sampling.decisioncore.gedik

import hu.elte.inf.sampling.core.{DynamicPartitioner, PartitionerConfig, PartitionerFactory, PartitioningInfo}
import hu.elte.inf.sampling.decisioncore.adaptive.{ConsistentHash, ConsistentHashPartitioner}
import hu.elte.inf.sampling.decisioncore.gedik.GedikPartitioner.PartitionerType

class GedikPartitioner(val partitionerType: PartitionerType,
                       override val numPartitions: Int,
                       val consistentHash: ConsistentHashPartitioner,
                       val currFrequencies: Map[Any, Double],
                       val prevFrequencies: Map[Any, Double],
                       val params: GedikPartitioner.Params,
                       val prevP: Option[Any => Int] = None)
  extends DynamicPartitioner {

  private val prevPartitioner: Any => Int =
    prevP match {
      case Some(p) => p
      case None => key: Any => consistentHash.consistentHash.getPartition(key)
    }

  private val internalPartitioner: Any => Int = partitionerType match {
    case GedikPartitioner.Scan =>
      new Scan(numPartitions, consistentHash, currFrequencies,
        prevFrequencies, prevPartitioner, params).createHash()
    case GedikPartitioner.Readj =>
      new Readj(numPartitions, consistentHash, currFrequencies,
        prevFrequencies, prevPartitioner, params).createHash()
    case GedikPartitioner.Redist =>
      new Redist(numPartitions, consistentHash, currFrequencies,
        prevFrequencies, prevPartitioner, params).createHash()
  }

  override def createUpdated(partitioningInfo: PartitioningInfo): DynamicPartitioner = {
    new GedikPartitioner(
      partitionerType,
      partitioningInfo.partitions,
      consistentHash,
      partitioningInfo.heavyKeys,
      currFrequencies,
      params,
      Some(internalPartitioner)
    )
  }

  override def getPartition(key: Any): Int =
    internalPartitioner(key)

}

object GedikPartitioner {

  trait PartitionerType

  case object Scan extends PartitionerType

  case object Readj extends PartitionerType

  case object Redist extends PartitionerType

  case class Config(numPartitions: Int, algorithm: String, replicationFactor: Int, params: GedikPartitioner.Params) extends PartitionerConfig

  case class Params(betaS: Double => Double,
                    betaC: Double => Double,
                    thetaS: Double,
                    thetaC: Double,
                    thetaN: Double,
                    utility: (Double, Double) => Double)

  object Factory extends PartitionerFactory[GedikPartitioner.Config] {
    override def apply(numPartitions: Int, config: GedikPartitioner.Config): GedikPartitioner = {

      val partitionerType =
        config.algorithm match {
          case "Scan" => Scan
          case "Readj" => Readj
          case "Redist" => Redist
        }

      new GedikPartitioner(
        partitionerType,
        numPartitions,
        new ConsistentHashPartitioner(
          Array.fill[Double](numPartitions)(1.0d / numPartitions),
          config.replicationFactor,
          ConsistentHash.defaultHashFunction),
        Map.empty[Any, Double],
        Map.empty[Any, Double],
        config.params,
      )
    }
  }

}
