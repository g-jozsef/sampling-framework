package hu.elte.inf.sampling.core

import scala.collection.mutable

trait Sampling extends Serializable {
  /**
   * Increments the version used for histogram versioning for this sampling
   */
  def incrementVersion(): Unit

  /**
   * Return the version of this sampling
   */
  def getVersion: Int

  /**
   * Over the lifetime of this sampler how much elements did it process
   */
  def getTotalProcessedElements: Long

  /**
   * Memory usage in bytes
   */
  def estimateMemoryUsage: Int

  /**
   * Process a data entry with a given key
   */
  def recordKey(key: Any, multiplicity: Int = 1): Unit

  /**
   * Returns an estimate of the frequencies
   */
  def estimateRelativeFrequencies(): (Long, mutable.Map[Any, Double])

  /**
   * Returns an estimate of the frequencies
   */
  def estimateProcessedKeyNumbers(): mutable.Map[Any, Long]
}
