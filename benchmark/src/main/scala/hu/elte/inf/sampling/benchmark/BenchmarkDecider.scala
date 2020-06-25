package hu.elte.inf.sampling.benchmark

import java.io.FileWriter

import hu.elte.inf.sampling.benchmark.OracleCalculator.NormalOracleCalulator
import hu.elte.inf.sampling.benchmark.StreamMaker.{BustyFileStreamMaker, FileStreamMaker, PooledStreamMaker}
import hu.elte.inf.sampling.benchmark.tools.Runner.DeciderRunner
import hu.elte.inf.sampling.core.{SamplerCase, _}
import hu.elte.inf.sampling.decisioncore.gedik.GedikPartitioner
import hu.elte.inf.sampling.decisioncore.{Decider, DeciderCase, NaiveDecider, gedik}
import hu.elte.inf.sampling.visualization.Plotter
import org.json4s.native.Serialization
import org.json4s.{DefaultFormats, Formats, NoTypeHints}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object BenchmarkDecider extends Logger with Multithreading {
  val rootDirectory: String = "D:\\datastreams\\"

  val deciderFactories: (String, Seq[Int => DeciderCase[Decider[Any], Any]]) = ("BaseDeciderFactories", Seq(
    (np: Int) => DeciderCase.fromFactory[NaiveDecider[Any, GedikPartitioner.Config], (PartitionerFactory[GedikPartitioner.Config], GedikPartitioner.Config, Int)](
      (GedikPartitioner.Factory, new gedik.GedikPartitioner.Config(
        np, "Scan", 100, GedikPartitioner.Params(
          betaS = x => x * 1.0,
          betaC = x => x * 1.0,
          thetaS = 0.2,
          thetaC = 0.2,
          thetaN = 0.2,
          // utility = (balancePenalty, migrationPenalty) => balancePenalty + migrationPenalty
          utility = (balancePenalty, _) => balancePenalty,
        )
      ), 2),
      x => new NaiveDecider[Any, GedikPartitioner.Config](x._1, x._2, x._3)),
  ))

  def main(args: Array[String]): Unit = {
    benchmarkDataset(30000, 300, 5, "dataset_C", TestCases.stateOfTheArt, None)
    benchmarkDataset(30000, 300, 5, "dataset_C", TestCases.stateOfTheArt,
      Some(BustyFileStreamMaker.Config(burstStartProbability = 0.3,
        keyBurstynessProbability = 0.2,
        burstLength = (2, 4))))
    benchmarkDataset(30000, 300, 5, "dataset_C", TestCases.stateOfTheArt,
      Some(BustyFileStreamMaker.Config(burstStartProbability = 0.20,
        keyBurstynessProbability = 0.1,
        burstLength = (3, 5))))

    benchmarkDataset(30000, 300, 10, "dataset_C", TestCases.stateOfTheArt, None)
    benchmarkDataset(30000, 300, 10, "dataset_C", TestCases.stateOfTheArt,
      Some(BustyFileStreamMaker.Config(burstStartProbability = 0.3,
        keyBurstynessProbability = 0.2,
        burstLength = (2, 4))))
    benchmarkDataset(30000, 300, 10, "dataset_C", TestCases.stateOfTheArt,
      Some(BustyFileStreamMaker.Config(burstStartProbability = 0.20,
        keyBurstynessProbability = 0.1,
        burstLength = (3, 5))))

    PooledStreamMaker.streamCache.clear()
    benchmarkDataset(30000, 300, 20, "dataset_C", TestCases.stateOfTheArt, None)
    benchmarkDataset(30000, 300, 20, "dataset_C", TestCases.stateOfTheArt,
      Some(BustyFileStreamMaker.Config(burstStartProbability = 0.3,
        keyBurstynessProbability = 0.2,
        burstLength = (2, 4))))
    benchmarkDataset(30000, 300, 20, "dataset_C", TestCases.stateOfTheArt,
      Some(BustyFileStreamMaker.Config(burstStartProbability = 0.20,
        keyBurstynessProbability = 0.1,
        burstLength = (3, 5))))

    benchmarkDataset(30000, 300, 50, "dataset_C", TestCases.stateOfTheArt, None)
    benchmarkDataset(30000, 300, 50, "dataset_C", TestCases.stateOfTheArt,
      Some(BustyFileStreamMaker.Config(burstStartProbability = 0.3,
        keyBurstynessProbability = 0.2,
        burstLength = (2, 4))))
    benchmarkDataset(30000, 300, 50, "dataset_C", TestCases.stateOfTheArt,
      Some(BustyFileStreamMaker.Config(burstStartProbability = 0.20,
        keyBurstynessProbability = 0.1,
        burstLength = (3, 5))))


    benchmarkDataset(30000, 300, 5, "dataset_C", TestCases.oursoluzion, None)
    benchmarkDataset(30000, 300, 5, "dataset_C", TestCases.oursoluzion,
      Some(BustyFileStreamMaker.Config(burstStartProbability = 0.3,
        keyBurstynessProbability = 0.2,
        burstLength = (2, 4))))
    benchmarkDataset(30000, 300, 5, "dataset_C", TestCases.oursoluzion,
      Some(BustyFileStreamMaker.Config(burstStartProbability = 0.20,
        keyBurstynessProbability = 0.1,
        burstLength = (3, 5))))

    benchmarkDataset(30000, 300, 10, "dataset_C", TestCases.oursoluzion, None)
    benchmarkDataset(30000, 300, 10, "dataset_C", TestCases.oursoluzion,
      Some(BustyFileStreamMaker.Config(burstStartProbability = 0.3,
        keyBurstynessProbability = 0.2,
        burstLength = (2, 4))))
    benchmarkDataset(30000, 300, 10, "dataset_C", TestCases.oursoluzion,
      Some(BustyFileStreamMaker.Config(burstStartProbability = 0.20,
        keyBurstynessProbability = 0.1,
        burstLength = (3, 5))))
    PooledStreamMaker.streamCache.clear()
    benchmarkDataset(30000, 300, 20, "dataset_C", TestCases.oursoluzion, None)
    benchmarkDataset(30000, 300, 20, "dataset_C", TestCases.oursoluzion,
      Some(BustyFileStreamMaker.Config(burstStartProbability = 0.3,
        keyBurstynessProbability = 0.2,
        burstLength = (2, 4))))
    benchmarkDataset(30000, 300, 20, "dataset_C", TestCases.oursoluzion,
      Some(BustyFileStreamMaker.Config(burstStartProbability = 0.20,
        keyBurstynessProbability = 0.1,
        burstLength = (3, 5))))

    benchmarkDataset(30000, 300, 50, "dataset_C", TestCases.oursoluzion, None)
    benchmarkDataset(30000, 300, 50, "dataset_C", TestCases.oursoluzion,
      Some(BustyFileStreamMaker.Config(burstStartProbability = 0.3,
        keyBurstynessProbability = 0.2,
        burstLength = (2, 4))))
    benchmarkDataset(30000, 300, 50, "dataset_C", TestCases.oursoluzion,
      Some(BustyFileStreamMaker.Config(burstStartProbability = 0.20,
        keyBurstynessProbability = 0.1,
        burstLength = (3, 5))))

    logInfo("Finished benchmarking!")
    service.shutdown()
  }


  def benchmarkDataset(microBatchSize: Int,
                       topK: Int,
                       numPartitions: Int,
                       dataSet: String,
                       testCases: (String, Seq[Int => SamplerCase[SamplingBase, AlgorithmConfiguration]]),
                       burstConfig: Option[BustyFileStreamMaker.Config]): Unit = {

    val directory: String = rootDirectory + dataSet
    val outDirectory: String = s"${rootDirectory}\\benchmarkdecider_${testCases._1}\\topk=${topK}_np=${numPartitions}_${dataSet}_${Utils.dateTime}\\"
    burstConfig.foreach(config => config.writeOut(outDirectory))
    val files = Utils.getFilesFromDirectory(directory)
    files.foreach {
      file =>
        logInfo("Deciding for file: " + file)
        val futureBalances: Seq[Future[(SamplerCase[SamplingBase, AlgorithmConfiguration], Int => DeciderCase[Decider[Any], Any], (String, Seq[Double]))]] =
          testCases._2.map(_ (numPartitions)).flatMap {
            testCase =>
              deciderFactories._2.map {
                deciderFactory =>
                  Future {
                    (
                      testCase,
                      deciderFactory,
                      benchmarkDecider[Sampling, AlgorithmConfiguration](deciderFactory(numPartitions), testCase, directory + "/" + file, microBatchSize, topK, burstConfig)
                    )
                  }
              }
          }

        val balances = futureBalances.map(x => Await.result(x, 24.hours))

        // Plot errors
        Plotter.line("",
          700,
          500,
          None,
          "% imbalance error",
          outDirectory + s"\\${file}_balance_error",
          true,
          balances.map(x => (x._3._2.map(x => x * 100.0D).iterator, x._3._1)): _*)

        val resultWriter = new FileWriter(outDirectory + s"\\${file}_meta.json")

        resultWriter.write(FormattedResult.serializeArray(numPartitions, balances))
        resultWriter.close()

        balances.map(x => x._3) foreach {
          case (description: String, imbalance: Seq[Double]) =>
            logInfo(s"$description: ${imbalance.mkString(", ")}")
        }

        logInfo("Finished deciding for file: " + file)
    }
  }


  def benchmarkDecider[S <: Sampling, C <: AlgorithmConfiguration](decider: DeciderCase[Decider[Any], Any],
                                                                   testCase: SamplerCase[S, C],
                                                                   file: String,
                                                                   windowSize: Int,
                                                                   topK: Int,
                                                                   burstConfig: Option[BustyFileStreamMaker.Config]): (String, Seq[Double]) = {

    val deciderStrategy = decider.makeAlgorithm
    // Initialize runners
    val runner = new SamplerRunner[Any, Array[Double]](
      topK: Int,
      testCase,
      new PooledStreamMaker(
        burstConfig match {
          case Some(config) => new BustyFileStreamMaker(file, windowSize, topK, Shop.fromByteBuffer, new NormalOracleCalulator[Any](Shop.fromInt), config)
          case None => new FileStreamMaker(file, windowSize, topK, Shop.fromByteBuffer, new NormalOracleCalulator[Any](Shop.fromInt))
        }
      ),
      new DeciderRunner[Any](deciderStrategy))

    logInfo("Started: " + decider.getConfig.toString + ", " + testCase.toString)
    val result = runner.run()
    logInfo("Finished: " + decider.getConfig.toString + ", " + testCase.toString)
    (testCase.getConfig.shortName, result.toSeq)
  }

  object FormattedResult {
    implicit val formats: Formats = DefaultFormats + NoTypeHints

    case class FormattedResult(testCase: String,
                               decider: String,
                               percentImbalances: Array[String])

    def serializeArray[T](np: Int, results: Seq[(SamplerCase[SamplingBase, AlgorithmConfiguration], Int => DeciderCase[Decider[Any], Any], (String, Seq[Double]))]): String = {
      Serialization.writePretty(results.map(
        result =>
          FormattedResult(
            testCase = result._1.getConfig.longName,
            decider = result._2(np).getConfig.toString,
            percentImbalances = result._3._2.map(x => (Utils.round(x, 6) * 100).toString).toArray
          ))
      )
    }
  }

}