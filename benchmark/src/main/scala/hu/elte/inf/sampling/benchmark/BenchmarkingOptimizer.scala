package hu.elte.inf.sampling.benchmark

import java.nio.file.{Files, Paths}

import hu.elte.inf.sampling.benchmark.BenchmarkingOptimizer.{TestCaseMaker, mean}
import hu.elte.inf.sampling.benchmark.StreamMaker.BustyFileStreamMaker
import hu.elte.inf.sampling.core.TypeDefs.TTime
import hu.elte.inf.sampling.core._
import hu.elte.inf.sampling.datagenerator.DStreamReader.ReaderFactory

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag
import scala.util.Random


class BenchmarkingOptimizer[T: ClassTag : Ordering](describe: String,
                                                    inputDir: String,
                                                    outputDir: String,
                                                    factory: ReaderFactory[T],
                                                    oracleCalculator: OracleCalculator[T],
                                                    windowSize: Int,
                                                    topK: Int,
                                                    upperRunningLimit: TTime,
                                                    useTimeoutForInitialConf: Boolean,
                                                    initialConfigPopulation: Seq[AlgorithmConfiguration],
                                                    evolutionStrategy: EvolutionStrategy[AlgorithmConfiguration],
                                                    murderStrategy: SelectStrategy[AlgorithmConfiguration, BenchmarkResult[T]],
                                                    testCaseMaker: TestCaseMaker[AlgorithmConfiguration],
                                                    errorFunction: ErrorFunction[T],
                                                    bursts: Seq[Option[BustyFileStreamMaker.Config]] = Seq(None))(
                                                     implicit executionContext: ExecutionContext)
  extends Logger {

  val rand = new Random()

  var current: Seq[(AlgorithmConfiguration, BenchmarkResult[T])] = {
    logInfo(s"[${describe}]: Running initial step...")
    run(outputDir + s"/initialPopulation_output${Utils.dateTime}/", 0, if (useTimeoutForInitialConf) Some(upperRunningLimit) else None, initialConfigPopulation)
  }

  def search(iter: Int): Unit = {
    (1 to iter) foreach {
      i =>
        logInfo(s"[${describe}]: Running iteration $i...")
        current = step(i, current.map(x => x._1))
    }
  }

  def step(i: Int, population: Seq[AlgorithmConfiguration]): Seq[(AlgorithmConfiguration, BenchmarkResult[T])] = {
    val newPopulation = evolutionStrategy.evolve(population)

    val o = outputDir + s"/${i}_output${Utils.dateTime}/"

    val results = run(o, i, Some(upperRunningLimit), newPopulation) ++ current

    val sorted = results.sortBy(x => x._2.fitness)

    val murdered = murderStrategy.select(sorted.map(x => (x._1, x._2)))

    val winner = sorted.head._2
    Files.createDirectories(Paths.get(o + "/winner/"))
    Benchmarking.processBenchmarkResult(Array(winner), o + "/winner/", backgroundRunning = true)

    murdered
  }

  def run(output: String, i: Int, upperRunningLimit: Option[TTime], configs: Seq[AlgorithmConfiguration]): Seq[(AlgorithmConfiguration, BenchmarkResult[T])] = {
    val testCases = testCaseMaker.makeTestCases(configs)

    bursts.flatMap(
      burst =>
        new BenchmarkingFromDirectory[T, AlgorithmConfiguration](s"${describe}_$i",
          windowSize,
          topK,
          inputDir,
          output,
          factory,
          oracleCalculator,
          testCases.toArray,
          upperRunningLimit,
          errorFunction,
          burst
        ).benchmark())
      .groupBy(x => x._1)
      .flatMap(x => x._2)
      .toSeq
      .map(x => (x._1.getConfig, mean(x._2)))
  }
}

object BenchmarkingOptimizer {

  abstract class TestCaseMaker[Config <: AlgorithmConfiguration]() {

    def makeTestCases(confs: Seq[Config]): Seq[SamplerCase[Sampling, Config]] = {
      confs.map({
        conf => makeTestCase(conf)
      })
    }

    def makeTestCase(conf: Config): SamplerCase[Sampling, Config]
  }

  /**
   *
   * Calculates the mean of the results from a configuration
   *
   * Assumes each result is from the same configuration!
   */
  def mean[T](results: Array[BenchmarkResult[T]]): BenchmarkResult[T] = {
    val noRes = results.length
    val aggregatederrors: Array[Double] = new Array(results.head.errors.length)

    var noTimes = 0
    for (i <- 0 until noRes) {
      Utils.addArrays(aggregatederrors, results(i).errors)
      noTimes += results(i).times.length
    }

    val times: Array[TimeBenchmarkResult] = new Array[TimeBenchmarkResult](noTimes)
    var k = 0
    for (i <- results.indices; j <- results(i).times.indices) {
      times(k) = results(i).times(j)
      k += 1
    }

    for (i <- aggregatederrors.indices) {
      aggregatederrors(i) = aggregatederrors(i) / noRes.toDouble
    }

    BenchmarkResult(results.head.config, times, results.head.result, aggregatederrors)
  }

  class AnyTestCaseMaker[+S <: Sampling : ClassTag, C <: AlgorithmConfiguration : ClassTag](factory: C => S)
    extends TestCaseMaker[C]() {
    override def makeTestCase(conf: C): SamplerCase[S, C] = {
      SamplerCase.fromFactory[S, C](conf, factory)
    }
  }

}
