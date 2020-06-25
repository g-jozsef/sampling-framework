package hu.elte.inf.sampling.benchmark.tools

import hu.elte.inf.sampling.benchmark.TimeBenchmarkResult

/** trait for metrics */
sealed trait Metric {
  def apply(measured: Seq[TimeBenchmarkResult]): TimeBenchmarkResult
}

object Metric {

  /** mean of measurements */
  object Mean extends Metric {
    override def apply(measured: Seq[TimeBenchmarkResult]): TimeBenchmarkResult = {

      val meanTime: Double = measured.map(x => x.time).sum / measured.length

      TimeBenchmarkResult(measured.flatMap(x => x.counters).toArray, meanTime)
    }
  }

}
