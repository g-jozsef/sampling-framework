package hu.elte.inf.sampling.core

/**
 * Creates a new sampling.
 *
 * Sampling specific configuration should be read from SparkEnv.get.conf
 */
trait SamplingFactory extends Serializable {
  def apply(): Sampling
}
