package hu.elte.inf.sampling.core

/**
 * An object that defines how the elements in a key-value pair RDD are partitioned by key.
 * Maps each key to a partition ID, from 0 to `numPartitions - 1`.
 *
 * Note that, partitioner must be deterministic, i.e. it must return the same partition id given
 * the same partition key.
 */
abstract class Partitioner extends Serializable {
  def numPartitions: Int

  def getPartition(key: Any): Int
}
