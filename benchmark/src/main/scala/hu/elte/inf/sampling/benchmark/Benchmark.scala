package hu.elte.inf.sampling.benchmark

import java.util.concurrent.{ExecutorService, Executors}

import hu.elte.inf.sampling.benchmark.BenchmarkingOptimizer.{AnyTestCaseMaker, TestCaseMaker}
import hu.elte.inf.sampling.benchmark.EvolutionStrategy.GeneticAlgorithm
import hu.elte.inf.sampling.benchmark.OracleCalculator.NormalOracleCalulator
import hu.elte.inf.sampling.benchmark.SelectStrategy.GeneticSelector
import hu.elte.inf.sampling.benchmark.StreamMaker.BustyFileStreamMaker
import hu.elte.inf.sampling.core.ErrorFunction.{HellingerDistance, KullbackLeiblerDivergence}
import hu.elte.inf.sampling.core.Shop.Type
import hu.elte.inf.sampling.core._
import hu.elte.inf.sampling.sampler.CheckpointSmoothed
import hu.elte.inf.sampling.sampler.lossycounting.LossyCounting
import hu.elte.inf.sampling.sampler.spacesaving.SpaceSaving
import hu.elte.inf.sampling.sampler.stickysampling.StickySampling

import scala.concurrent.ExecutionContext

object Benchmark extends OptionHandler {
  private val usage: String =
    """
      | Usage: sambem -i string -o string -m {opt/bmdir/dmdiraggr} [-ws int] [-d string] [-tr string] [-d int] [-e {kld/hel}] [-to int]
      |
      | [...] : optional parameters
      |
      | Required parameters:
      |         -ws: WindowSize
      |         -o: Output directory
      |         -i: Input directory
      |         -d: Description
      |         -tr: Threadcount
      |         -n: Name
      |         -to: Timeout
      |         -e: Errortype
      |         -tk: TopK
      |         -p: Number of partitions
      |
      | Required parameters:
      |        -i: Input directory
      |        -o: Output directory
      |        -m: Mode {opt - Optimize an algorithm, bmdir - Benchmark an algorithm against a directory, dmdiraggr - Benchmark an algorithm's aggregation against a directory}
      |
      | Parameters with default values:
      |        -d: Description (will default to output directory if not given
      |        -tr: Threadcount - how many threads to use for calculations, default 5
      |        -ws: WindowSize - frequency of measurements, default 30000
      |        -e: Errorfunction to use for calculating error, default is KullbackLeiblerDivergence
      |        -to: Timeout for optimizer in ms, default is 20000
      |
      | Arguments can be set in any order
      |""".stripMargin

  object Modes extends Enumeration {
    val BenchmarkDir, Optimize, BenchmarkDirAggr = Value
  }

  private val options: OptionFactoryMap = Map(
    "-m" -> (Symbol("mode"), {
      case "opt" => Modes.Optimize
      case "bmdir" => Modes.BenchmarkDir
      case "bmdiraggr" => Modes.BenchmarkDirAggr
      case unknown => throw new IllegalArgumentException(s"Unknown mode: $unknown")
    }),
    "-ws" -> (Symbol("windowsize"), (x: String) => x.toInt),
    "-o" -> (Symbol("outputdir"), (x: String) => x),
    "-i" -> (Symbol("inputdir"), (x: String) => x),
    "-tr" -> (Symbol("threads"), (x: String) => x.toInt),
    "-n" -> (Symbol("name"), (x: String) => x),
    "-to" -> (Symbol("timeout"), (x: String) => x.toInt),
    "-e" -> (Symbol("errtype"), {
      case "kld" => new KullbackLeiblerDivergence()
      case "hel" => new HellingerDistance()
      case unknown => throw new IllegalArgumentException(s"Unknown mode: $unknown")
    }),
    "-tk" -> (Symbol("topK"), (x: String) => x.toInt),
  )

  private val defaultOptions: OptionMap = Map(
    Symbol("windowsize") -> 30000,
    Symbol("errtype") -> new HellingerDistance(),
    Symbol("threads") -> 5,
    Symbol("timeout") -> 200000
  )

  override def getUsage: String = usage

  override def getOptions: Map[String, (Symbol, String => Any)] = options

  override def getDefaultOptions: OptionMap = defaultOptions


  def main(args: Array[String]): Unit = {
    readOptions(args)

    val mode: Modes.Value = getOption(Symbol("mode"))
    val windowSize: Int = getOption(Symbol("windowsize"))
    val outputDir: String = getOption(Symbol("outputdir"))
    val topK: Int = getOption(Symbol("topK"))
    val inputDir: String = getOption(Symbol("inputdir"))
    val threads: Int = getOption(Symbol("threads"))
    val name: String = getOption(Symbol("name"))
    val timeout: Int = getOption(Symbol("timeout"))
    val errType: ErrorFunction[Shop.Type] = getOption(Symbol("errtype"))


    val service = Executors.newFixedThreadPool(threads)
    implicit val ec: ExecutionContext = new ExecutionContext {
      val threadPool: ExecutorService = service

      override def reportFailure(cause: Throwable): Unit = {}

      override def execute(runnable: Runnable): Unit = threadPool.submit(runnable)

    }

    mode match {
      case Modes.BenchmarkDir =>
        benchmark(inputDir, outputDir, windowSize, 200, 1, "dataset_A", TestCases.stateOfTheArt, errType, None)
        benchmark(inputDir, outputDir, windowSize, 200, 5, "dataset_A", TestCases.stateOfTheArt, errType, None)
        benchmark(inputDir, outputDir, windowSize, 200, 10, "dataset_A", TestCases.stateOfTheArt, errType, None)
        benchmark(inputDir, outputDir, windowSize, 200, 50, "dataset_A", TestCases.stateOfTheArt, errType, None)
        benchmark(inputDir, outputDir, windowSize, 200, 1, "dataset_A", TestCases.stateOfTheArt, errType, Some(BustyFileStreamMaker.Config()))
        benchmark(inputDir, outputDir, windowSize, 200, 5, "dataset_A", TestCases.stateOfTheArt, errType, Some(BustyFileStreamMaker.Config()))
        benchmark(inputDir, outputDir, windowSize, 200, 10, "dataset_A", TestCases.stateOfTheArt, errType, Some(BustyFileStreamMaker.Config()))
        benchmark(inputDir, outputDir, windowSize, 200, 50, "dataset_A", TestCases.stateOfTheArt, errType, Some(BustyFileStreamMaker.Config()))

        benchmark(inputDir, outputDir, windowSize, 200, 1, "dataset_A", TestCases.stateOfTheArt2, errType, None)
        benchmark(inputDir, outputDir, windowSize, 200, 5, "dataset_A", TestCases.stateOfTheArt2, errType, None)
        benchmark(inputDir, outputDir, windowSize, 200, 10, "dataset_A", TestCases.stateOfTheArt2, errType, None)
        benchmark(inputDir, outputDir, windowSize, 200, 50, "dataset_A", TestCases.stateOfTheArt2, errType, None)
        benchmark(inputDir, outputDir, windowSize, 200, 1, "dataset_A", TestCases.stateOfTheArt2, errType, Some(BustyFileStreamMaker.Config()))
        benchmark(inputDir, outputDir, windowSize, 200, 5, "dataset_A", TestCases.stateOfTheArt2, errType, Some(BustyFileStreamMaker.Config()))
        benchmark(inputDir, outputDir, windowSize, 200, 10, "dataset_A", TestCases.stateOfTheArt2, errType, Some(BustyFileStreamMaker.Config()))
        benchmark(inputDir, outputDir, windowSize, 200, 50, "dataset_A", TestCases.stateOfTheArt2, errType, Some(BustyFileStreamMaker.Config()))
      case Modes.Optimize =>
//        optimize(windowSize, topK, outputDir, inputDir, name, timeout, errType,
//          Seq(CheckpointSmoothed.Config(SamplerCase.fromFactory[LossyCounting, LossyCounting.Config](
//            LossyCounting.Config(), new LossyCounting(_: LossyCounting.Config)),
//            40000,
//            40000,
//            0.33
//          )),
//          new AnyTestCaseMaker[CheckpointSmoothed[LossyCounting, LossyCounting.Config], AlgorithmConfigurattoion](CheckpointSmoothed.apply[LossyCounting, LossyCounting.Config](_)),
//          new RandomLocalMinimumSearchSelector[AlgorithmConfiguration, BenchmarkResult[Shop.Type]](),
//          new RandomLocalMinimumSearch(10)
//        )
        optimize(windowSize, topK, outputDir, inputDir, name, timeout, errType,
          Seq(CheckpointSmoothed.Config(SamplerCase.fromFactory[LossyCounting, LossyCounting.Config](
            LossyCounting.Config(), new LossyCounting(_: LossyCounting.Config)),
            40000,
            40000,
            0.33
          )),
          new AnyTestCaseMaker[CheckpointSmoothed[LossyCounting, LossyCounting.Config], AlgorithmConfiguration](CheckpointSmoothed.apply[LossyCounting, LossyCounting.Config](_)),
          new GeneticSelector(1, 10),
          new GeneticAlgorithm(4, 0.3)
        )
      case Modes.BenchmarkDirAggr =>
        benchmarkDirAggr(windowSize, topK, outputDir, inputDir, name, errType)
    }


    service.shutdown()
  }

  private def optimize(windowSize: Int,
                       topK: Int,
                       outputDir: String,
                       inputDir: String,
                       name: String,
                       timeout: Int,
                       errType: ErrorFunction[Shop.Type],
                       startingConfig: Seq[AlgorithmConfiguration],
                       testCaseMaker: TestCaseMaker[AlgorithmConfiguration],
                       murderStrategy: SelectStrategy[AlgorithmConfiguration, BenchmarkResult[Shop.Type]],
                       evolutionStrategy: EvolutionStrategy[AlgorithmConfiguration])(implicit executionContext: ExecutionContext): Unit = {

    val optimizer: BenchmarkingOptimizer[Shop.Type] = new BenchmarkingOptimizer(
      name,
      inputDir,
      outputDir + s"_${Utils.dateTime}",
      Shop.fromByteBuffer,
      new NormalOracleCalulator[Type](Shop.fromInt),
      windowSize,
      topK,
      timeout,
      true,
      startingConfig,
      evolutionStrategy,
      murderStrategy,
      testCaseMaker,
      errType)
    optimizer.search(1000)
  }

  def benchmark(rootDirectory: String,
                outputRootDirectory: String,
                microbatchSize: Int,
                topK: Int,
                numPartitions: Int,
                dataset: String,
                testCases: (String, Seq[Int => SamplerCase[SamplingBase, AlgorithmConfiguration]]),
                errorFunction: ErrorFunction[Shop.Type],
                burstConfig: Option[BustyFileStreamMaker.Config])(implicit ec: ExecutionContext): Unit = {
    val outputDir: String = s"${outputRootDirectory}\\${Utils.getObjectSimpleName(errorFunction)}_${testCases._1}\\topk=${topK}_np=${numPartitions}_$dataset${if (burstConfig.isEmpty) "" else "_bursty"}_${Utils.dateTime}\\"
    burstConfig.foreach(config => config.writeOut(outputDir))
    new BenchmarkingFromDirectory[Shop.Type, AlgorithmConfiguration](s"${Utils.getObjectSimpleName(errorFunction)} of dataset testcases: ${testCases._1} ${dataset} with topK = ${topK} and numpartitions = ${numPartitions}",
      microbatchSize,
      topK,
      rootDirectory + dataset,
      outputDir,
      Shop.fromByteBuffer,
      new NormalOracleCalulator[Shop.Type](Shop.fromInt),
      testCases._2.map(_ (numPartitions)).toArray,
      None,
      errorFunction,
      burstConfig
    ).benchmark()
  }

  private def benchmarkDir(windowSize: Int, topK: Int, outputDir: String, inputDir: String, name: String, errType: ErrorFunction[Shop.Type])(implicit executionContext: ExecutionContext): Unit = {
    new BenchmarkingFromDirectory[Shop.Type, AlgorithmConfiguration](name,
      windowSize,
      topK,
      inputDir,
      outputDir,
      Shop.fromByteBuffer,
      new NormalOracleCalulator[Type](Shop.fromInt),
      Array(
        SamplerCase.fromFactory[StickySampling, StickySampling.Config](StickySampling.Config(), new StickySampling(_: StickySampling.Config)),
        SamplerCase.fromFactory[LossyCounting, LossyCounting.Config](LossyCounting.Config(), new LossyCounting(_: LossyCounting.Config)),
        SamplerCase.fromFactory[ SpaceSaving, SpaceSaving.Config](SpaceSaving.Config(0.0001), new SpaceSaving(_: SpaceSaving.Config)),
      ),
      None,
      errType,
      None
    ).benchmark()
  }

  private def benchmarkDirAggr(windowSize: Int, topK: Int, outputDir: String, inputDir: String, name: String, errType: ErrorFunction[Shop.Type])(implicit executionContext: ExecutionContext): Unit = {
    new BenchmarkingFromDirectory[Shop.Type, AlgorithmConfiguration](name,
      windowSize,
      topK,
      inputDir,
      outputDir,
      Shop.fromByteBuffer,
      new NormalOracleCalulator[Type](Shop.fromInt),
      Array(
        SamplerCase.fromFactory[StickySampling, StickySampling.Config](StickySampling.Config(), new StickySampling(_: StickySampling.Config)),
        SamplerCase.fromFactory[LossyCounting, LossyCounting.Config](LossyCounting.Config(), new LossyCounting(_: LossyCounting.Config)),
      ),
      None,
      errType,
      None
    ).benchmark()
  }
}
