package hu.elte.inf.sampling.benchmark

import java.io.FileWriter

import hu.elte.inf.sampling.benchmark.tools.Runner.BenchmarkerRunner
import hu.elte.inf.sampling.core.{SamplerCase, _}
import hu.elte.inf.sampling.visualization.Plotter

import scala.reflect.ClassTag

class Benchmarking[T: ClassTag : Ordering](topK: Int,
                                           val backgroundRunning: Boolean,
                                           testCase: SamplerCase[Sampling, AlgorithmConfiguration],
                                           streamMaker: StreamMaker[T],
                                           val errorFunction: ErrorFunction[T],
                                           benchmarkerRunner: BenchmarkerRunner[T])
  extends SamplerRunner[T, (TimeBenchmarkResult, SamplerResult[T])](topK, testCase, streamMaker, benchmarkerRunner) {

  final def calculateBenchmarkResult()(implicit oracle: SamplerResult[T]): BenchmarkResult[T] = {
    var start = System.nanoTime()
    val runs = (1 to 10).map(_ => run())
    val tbr = runs.map(x => x._1).map(x => x.time).sum.toDouble / runs.length
    val (timeBenchmarkResult, samplingResult) = (new TimeBenchmarkResult(runs.head._1.counters, tbr), runs.head._2)
    //val (timeBenchmarkResult, samplingResult) = run()
    logDebug(s"Sampling finished for ${testCase.toString}, it took ${(System.nanoTime() - start).toDouble / 1000000} ms")
    timeBenchmarkResult.print()

    logDebug(s"Calculating error for ${testCase.toString}:")
    start = System.nanoTime()
    val mseError = errorFunction.calculateError(oracle, samplingResult)
    logDebug(s"Calculated error for ${testCase.toString}, it took ${(System.nanoTime() - start).toDouble / 1000000} ms")

    val res = BenchmarkResult({
      testCase.getConfig
    }, Array[TimeBenchmarkResult](timeBenchmarkResult), samplingResult, mseError)
    res
  }

  final def benchmark(): BenchmarkResult[T] = {
    logDebug(s"STARTING BENCHMARKING...")

    val benchmarkResult = calculateBenchmarkResult()(stream.oracle)

    logDebug(s"ENDED BENCHMARKING...")
    benchmarkResult
  }
}

object Benchmarking extends Logger {
  def processBenchmarkResult[T](results: Array[BenchmarkResult[T]], fileOutput: String, backgroundRunning: Boolean): Unit = {
    logDebug("Processing results... " + fileOutput)
    Plotter.line("", 700, 500, Some(0, 1), "Hellinger distance", fileOutput + "_error", backgroundRunning, results.map(x => (x.errors.iterator, x.config.shortName)).toSeq: _*)
    if (results.head.times.length == 1) {
      Plotter.line("", 700, 500, None, "counters", fileOutput + "_counters_per_window", backgroundRunning, results.map(x => (x.times.head.counters.map(c => c.toDouble).iterator, x.config.shortName)).toSeq: _*)
    }
    Plotter.box("", "ms", 500, 500, fileOutput + "_times", backgroundRunning, false, results.map(x => (x.times.map(t => t.time).iterator, x.config.shortName)).toSeq: _*)
    Plotter.box("", "#", 500, 500, fileOutput + "_counters", backgroundRunning, false, results.map(x => (x.times.flatMap(t => t.counters.map(x => x.toDouble)).iterator, x.config.shortName)).toSeq: _*)

    if (fileOutput != "") {
      val resultWriter = new FileWriter(fileOutput + "_" + Utils.dateTime + ".json")

      resultWriter.write(BenchmarkResult.serializeArray(results.toSeq))
      resultWriter.close()
    }
    logDebug("Results processed... " + fileOutput)
  }
}