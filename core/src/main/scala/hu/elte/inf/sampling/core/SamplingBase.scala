package hu.elte.inf.sampling.core

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

abstract class SamplingBase(val config: AlgorithmConfiguration)
  extends Sampling {
  protected val version: AtomicInteger = new AtomicInteger(0)

  /**
   * Returns an estimate of the frequencies
   */
  override def estimateRelativeFrequencies(): (Long, mutable.Map[Any, Double]) = {
    val estimatedNumbers = estimateProcessedKeyNumbers()
    val sum: Double = estimatedNumbers.values.sum.toDouble
    (sum.toLong,
      mutable.Map.empty[Any, Double] ++ estimatedNumbers.view.mapValues(x => x.toDouble / sum))
  }

  /**
   * Increments the version used for histogram versioning for this sampling
   */
  override def incrementVersion(): Unit = version.incrementAndGet()

  /**
   * Return the version of this sampling
   */
  override def getVersion(): Int = version.get()
}
