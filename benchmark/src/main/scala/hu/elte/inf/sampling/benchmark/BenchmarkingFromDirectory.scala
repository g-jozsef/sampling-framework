package hu.elte.inf.sampling.benchmark

import java.nio.file.{Files, Paths}
import java.util.concurrent.atomic.AtomicInteger

import hu.elte.inf.sampling.benchmark.BenchmarkingFromDirectory.mean
import hu.elte.inf.sampling.benchmark.StreamMaker.{BustyFileStreamMaker, FileStreamMaker, PooledStreamMaker}
import hu.elte.inf.sampling.benchmark.tools.Metric
import hu.elte.inf.sampling.benchmark.tools.Runner.BenchmarkerRunner
import hu.elte.inf.sampling.core.TypeDefs.TTime
import hu.elte.inf.sampling.core.{SamplerCase, _}
import hu.elte.inf.sampling.datagenerator.DStreamReader.ReaderFactory

import scala.collection.parallel.CollectionConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag

class BenchmarkingFromDirectory[T: ClassTag : Ordering, Config <: AlgorithmConfiguration](shortName: String,
                                                                                          windowSize: Int,
                                                                                          topK: Int,
                                                                                          directory: String,
                                                                                          outputDir: String,
                                                                                          factory: ReaderFactory[T],
                                                                                          oracleCalculator: OracleCalculator[T],
                                                                                          testCases: Array[SamplerCase[Sampling, Config]],
                                                                                          upperRunningLimit: Option[TTime],
                                                                                          errorFunction: ErrorFunction[T],
                                                                                          burstConfig: Option[BustyFileStreamMaker.Config])(
                                                                                           implicit executionContext: ExecutionContext) extends Logger {


  var progression: AtomicInteger = new AtomicInteger(0)
  var totalBenchmarksNeeded: Option[Int] = None

  def setTotalBenchmarksNeeded(total: Int): Unit = synchronized {
    if (totalBenchmarksNeeded.isEmpty) {
      totalBenchmarksNeeded = Some(total)
      logInfo(s"Total number of tests and files combination is $total")
    }
  }

  def recalculateBenchmarksEstimate(cut: Int): Unit = synchronized {
    val newVal = totalBenchmarksNeeded.get - cut
    totalBenchmarksNeeded = Some(newVal)
    logInfo(s"Total number of tests and files combination changes, some tests have been cut, new total is: $newVal")
  }

  def reportProgression(time: TTime, testCase: SamplerCase[Sampling, Config], file: String): Unit = synchronized {
    val prog = progression.incrementAndGet()
    logInfo(s"Benchmark $prog of ${totalBenchmarksNeeded.get} (${testCase.toString}) finished for file ${file} and sampling took approximately $time ms!")
  }

  def reportStart(testCase: SamplerCase[Sampling, Config], file: String): Unit = synchronized {
    logInfo(s"Started benchmark (${testCase.toString}) for file ${file}")
  }

  def benchmark(): Array[(SamplerCase[Sampling, Config], Array[BenchmarkResult[T]])] = {
    println("\n")
    logDebug("STARTING BATCH PROCESSING, " + shortName)
    logDebug("Collecting files from " + directory)
    logInfo("Calculating progression...")

    val files = Utils.getFilesFromDirectory(directory)

    setTotalBenchmarksNeeded(files.length * testCases.length)

    // case1: f11, f12, f13, f14;
    // case2: f21, f22, f23, f24
    val results: Array[(SamplerCase[Sampling, Config], Array[BenchmarkResult[T]])] = upperRunningLimit match {
      // If we don't have an upper limit start every calculation in paralell and wait them all
      case None =>
        val futures = new Array[(SamplerCase[Sampling, Config], Array[Future[BenchmarkResult[T]]])](testCases.length)
        for (i <- futures.indices) {
          futures(i) = (testCases(i), startTestCasesParallel(testCases(i), files))
        }
        val awaited = new Array[(SamplerCase[Sampling, Config], Array[BenchmarkResult[T]])](futures.length)
        for (i <- awaited.indices) {
          awaited(i) = (futures(i)._1, futures(i)._2.map(future => Await.result(future, 24.hours)))
        }
        awaited
      // If we have an upper limit start only every case separately and wait for the results
      case Some(time) =>
        testCases
          .map(testCase => (testCase, Future {
            calculateTestCasesWithTimeout(testCase, files, time)
          }))
          .map(future => (future._1, Await.result(future._2, 24.hours)))
          .filterNot(x => x._2.isEmpty)
          .map(x => (x._1, x._2.get))
    }

    logInfo(s"Processing output...")

    Files.createDirectories(Paths.get(outputDir))

    if (results.nonEmpty) {
      // f11, f21
      val perFileResults = new Array[Array[BenchmarkResult[T]]](files.length)
      for (i <- files.indices) {
        val perFileResult = new Array[BenchmarkResult[T]](results.length)
        for (j <- perFileResult.indices) {
          perFileResult(j) = results(j)._2(i)
        }
        perFileResults(i) = perFileResult
      }
      (perFileResults zip files).par.foreach(
        s =>
          Benchmarking.processBenchmarkResult(s._1, outputDir + "/" + s._2, backgroundRunning = true)
      )

      Files.createDirectories(Paths.get(outputDir + "/aggregated/"))
      Benchmarking.processBenchmarkResult(mean(perFileResults), outputDir + "/aggregated/", backgroundRunning = true)
    }
    logInfo("Processed outputs")
    results
  }

  def calculateTestCasesWithTimeout(testCase: SamplerCase[Sampling, Config], files: Array[String], time: TTime): Option[Array[BenchmarkResult[T]]] = {
    val result = new Array[BenchmarkResult[T]](files.length)
    for (i <- files.indices) {
      val benchmarkResult = runTestCase(testCase, files(i))
      if (Metric.Mean(benchmarkResult.times.toSeq).time > time) {
        logDebug("Returning early, processing took too long already")
        recalculateBenchmarksEstimate(files.length - (i + 1))
        return None
      }
      result(i) = benchmarkResult
    }
    Some(result)
  }

  def startTestCasesParallel(testCase: SamplerCase[Sampling, Config], files: Array[String]): Array[Future[BenchmarkResult[T]]] = {
    files
      .map(file => {
        Future {
          runTestCase(testCase, file)
        }
      })
  }

  def runTestCase(testCase: SamplerCase[Sampling, Config], file: String): BenchmarkResult[T] = {
    reportStart(testCase, file)
    val result = new Benchmarking[T](
      topK,
      true,
      testCase,
      new PooledStreamMaker(
        burstConfig match {
          case Some(config) =>
            new BustyFileStreamMaker[T](
              directory + "/" + file,
              windowSize,
              topK,
              factory,
              oracleCalculator,
              config
            )
          case None =>
            new FileStreamMaker(
              directory + "/" + file,
              windowSize,
              topK,
              factory,
              oracleCalculator
            )
        }
      ),
      errorFunction,
      new BenchmarkerRunner[T]()
    ).benchmark()
    val time = Metric.Mean(result.times.toSeq)
    reportProgression(time.time, testCase, file)
    result
  }
}

object BenchmarkingFromDirectory {

  /**
   *
   * Calculates the mean of the results from a file
   *
   * Assumes each result is for the same file from different configurations
   */
  def mean[T](results: Array[Array[BenchmarkResult[T]]]): Array[BenchmarkResult[T]] = {
    val noRes = results.length
    val noResKind = results.head.length
    val result = new Array[BenchmarkResult[T]](noResKind)
    for (kind <- 0 until noResKind) {
      val aggregatedErrors: Array[Double] = new Array(results.head.head.errors.length)

      var noTimes = 0
      for (i <- 0 until noRes) {
        Utils.addArrays(aggregatedErrors, results(i)(kind).errors)
        noTimes += results(i)(kind).times.length
      }

      val times: Array[TimeBenchmarkResult] = new Array[TimeBenchmarkResult](noTimes)
      var k = 0
      for (i <- results.indices; j <- results(i)(kind).times.indices) {
        times(k) = results(i)(kind).times(j)
        k += 1
      }

      for (i <- aggregatedErrors.indices) {
        aggregatedErrors(i) = aggregatedErrors(i) / noRes.toDouble
      }

      result(kind) = BenchmarkResult(results.head(kind).config, times, results.head(kind).result, aggregatedErrors)
    }
    result
  }
}
