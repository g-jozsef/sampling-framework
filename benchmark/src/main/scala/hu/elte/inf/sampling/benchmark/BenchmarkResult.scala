package hu.elte.inf.sampling.benchmark

import hu.elte.inf.sampling.benchmark.BenchmarkResult.formats
import hu.elte.inf.sampling.core.{AlgorithmConfiguration, SamplerResult}
import org.json4s.native.Serialization
import org.json4s.{DefaultFormats, Formats, NoTypeHints}

case class BenchmarkResult[T](config: AlgorithmConfiguration,
                              times: Array[TimeBenchmarkResult],
                              result: SamplerResult[T],
                              errors: Array[Double]) {
  override def toString: String = {
    Serialization.writePretty(this)
  }

  lazy val fitness: Double = {
    errors.sum
  }
}

object BenchmarkResult {
  implicit val formats: Formats = DefaultFormats + NoTypeHints

  case class FormattedResult(describe: String,
                             raw: Array[String],
                             freq: Array[String],
                             times: Seq[Double],
                             counters: Seq[Int],
                             driftsDetected: Seq[Double],
                             errors: Seq[Double],
                             fitness: Double)

  def serializeArray[T](results: Seq[BenchmarkResult[T]]): String = {
    Serialization.writePretty(results.map((result: BenchmarkResult[T]) =>
      FormattedResult(
        describe = result.config.longName,
        raw = result.result.directOutput.zipWithIndex.map(x => s"window=${x._2}: ${x._1.mkString(",")}"),
        freq = result.result.relativeFreq.zipWithIndex.map(x => s"window=${x._2}: ${x._1.mkString(",")}"),
        times = result.times.map(x => x.time).toSeq,
        counters = result.times.flatMap(x => x.counters).toSeq,
        driftsDetected = result.result.drifts.toSeq,
        errors = result.errors.toSeq,
        fitness = result.fitness
      ))
    )
  }
}