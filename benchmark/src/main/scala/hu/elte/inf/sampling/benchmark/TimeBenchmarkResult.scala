package hu.elte.inf.sampling.benchmark

import hu.elte.inf.sampling.core.Logger
import hu.elte.inf.sampling.core.TypeDefs.TTime

case class TimeBenchmarkResult(counters: Array[Int],
                               time: TTime)

  extends Logger
    with Ordered[TimeBenchmarkResult] {

  def print(): TimeBenchmarkResult = {

    logDebug(s"\tNumber of counters used: [${counters.mkString(",")}]")
    logDebug(s"\tTime taken: [${time} ms]")

    this
  }

  override def compare(that: TimeBenchmarkResult): Int = {
    time.compare(that.time)
  }
}
