package hu.elte.inf.sampling.datagenerator

import hu.elte.inf.sampling.benchmark.OracleCalculator.NormalOracleCalulator
import hu.elte.inf.sampling.benchmark.StreamMaker.BustyFileStreamMaker
import hu.elte.inf.sampling.benchmark.{BenchmarkingFromDirectory, Multithreading, TestCases}
import hu.elte.inf.sampling.core.ErrorFunction.HellingerDistance
import hu.elte.inf.sampling.core.{SamplerCase, _}

object BenchmarkDirectory extends Logger with Multithreading {
  val rootDirectory: String = "D:\\datastreams\\"

  def main(args: Array[String]): Unit = {
    logInfo("NOBURST")
    benchmark(30000, 300, 1, "dataset_C", TestCases.oursoluzion, new HellingerDistance(), None)
    logInfo("HEAVYBURST")
    benchmark(30000, 300, 1, "dataset_C", TestCases.oursoluzion, new HellingerDistance(),
      Some(BustyFileStreamMaker.Config(burstStartProbability = 0.3,
        keyBurstynessProbability = 0.2,
        burstLength = (2, 4))))
    logInfo("LIGHTBURST")
    benchmark(30000, 300, 1, "dataset_C", TestCases.oursoluzion, new HellingerDistance(),
      Some(BustyFileStreamMaker.Config(burstStartProbability = 0.20,
        keyBurstynessProbability = 0.1,
        burstLength = (3, 5))))


    logInfo("Finished benchmarking!")
    service.shutdown()
  }

  def benchmark(microbatchSize: Int,
                topK: Int,
                numPartitions: Int,
                dataset: String,
                testCases: (String, Seq[Int => SamplerCase[SamplingBase, AlgorithmConfiguration]]),
                errorFunction: ErrorFunction[Shop.Type],
                burstConfig: Option[BustyFileStreamMaker.Config]): Unit = {
    val outputDir: String = s"${rootDirectory}\\${Utils.getObjectSimpleName(errorFunction)}_${testCases._1}\\topk=${topK}_np=${numPartitions}_${dataset}${if (burstConfig.isEmpty) "" else "_bursty"}_${Utils.dateTime}\\"
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

}
