package hu.elte.inf.sampling.core

/**
 * Creates a new partitioner.
 *
 * Partitioner specific configurations should be read from SparkEnv.get.conf
 */
trait PartitionerFactory[C <: PartitionerConfig] extends Serializable {
  def apply(numPartitions: Int, config: C): DynamicPartitioner
}
