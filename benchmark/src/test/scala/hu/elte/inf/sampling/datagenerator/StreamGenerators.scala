package hu.elte.inf.sampling.datagenerator

import hu.elte.inf.sampling.core.{Distribution, Shop, StreamConfig, Utils}
import hu.elte.inf.sampling.visualization.Plotter

import scala.collection.mutable
import scala.collection.parallel.CollectionConverters._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.util.Random

object StreamGenerators {
  val nShops = 100000
  implicit val nElements: Int = 5000000
  val rand: Random = new Random()
  val directory: String = "D:/datastreams/"

  def getNewNameFor(dataset: String, shortName: String): String = synchronized {
    s"$dataset/${shortName}_${Utils.dateTime}"
  }

  def main(args: Array[String]): Unit = {
    // forFigures()
    dataset_A()
    // dataset_B()
    // zeta("burst_demo", "burst_demo", "burst_demo", 2)
  }

  def forFigures(): Unit = {

    val initialDist = getDistribution("zeta", 10, 3)
    val initialStream = new DistributionShopStreamGenerator(initialDist, rand.nextInt())

    initialStream.restart()
    var lastScrambling: Array[Int] = initialStream.getShopMapping

    val drifters = Seq(3).map {
      x =>
        val distribution = getDistribution("zeta", 10, 3)
        var distrStreamGenerator: Option[Stream[Shop.Type]] = None

        while (distrStreamGenerator.isEmpty) {
          val d = new DistributionShopStreamGenerator(distribution, rand.nextInt())

          // Note it's safe to restart it here ONLY BECAUSE it is a seeded streamgenerator!
          d.restart()

          val currentScrambling = d.getShopMapping

          if (sufficientlyScrumbled(lastScrambling, currentScrambling)) {
            distrStreamGenerator = Some(d)
            lastScrambling = currentScrambling
          }
        }
        Drifter(distrStreamGenerator.get, 0.5, 60)
    }

    val stream = ConceptDriftStream(
      initialStream,
      drifters.head,
      drifters.tail: _*
    )
    stream.init()
    val generated = Array.fill(nElements) {
      stream.next()
    }

    val title = "Figure"

    val config: StreamConfig = StreamConfigFactory(
      title,
      nElements,
      nShops,
      getNewNameFor("figure", "figure"),
      initialStream,
      drifters: _*)

    Plotter.scatter(title, 1280, 720, directory + config.fileName, backgroundRunning = true, nElements, generated)

    DStreamWriter.write(config, directory + config.fileName, generated)
  }

  def dataset_B(): Unit = {
    val currentDataSet = "dataset_B"

    val combinations: Seq[(Double, Int)] = for {
      e <- Seq(0.3D, 0.4D, 0.5D, 0.6D, 0.7D, 0.8D, 0.9D, 1.0D)
      windowSize <- Seq(1)
    } yield (e, windowSize)


    val parComb = combinations
      .par

    parComb.tasksupport = new ForkJoinTaskSupport(
      new java.util.concurrent.ForkJoinPool(5))

    parComb.foreach {
      case (exp: Double, window: Int) =>
        zetas(currentDataSet,
          short = s"mid_drift_${Utils.round(exp, 5)}_$window",
          decs = s"One drift at the middle of the stream with zeta distribution, exponent = $exp and window = $window. " +
            s"Intended to measure the imbalance created by the use of a false sampling algorithm. " +
            s"Date of generation: ${Utils.dateTime}",
          initial = exp,
          (exp, 0.5, window))
    }
  }

  def dataset_A(): Unit = {
    val currentDataSet = "dataset_C"

    val combinations: Seq[(Double, Int)] = for {
      e <- Seq(0.8D, 1.0D, 1.5D, 2D)
      windowSize <- Seq(1, 1000000)
    } yield (e, windowSize)

    val parComb = combinations
      .par

    parComb.tasksupport = new ForkJoinTaskSupport(
      new java.util.concurrent.ForkJoinPool(8))

    parComb.foreach {
      case (exp: Double, window: Int) =>
        zetas(currentDataSet,
          short = s"mid_drift_${Utils.round(exp, 5).toString.replace(".", "d")}_$window",
          decs = s"One drift at the middle of the stream with zeta distribution, exponent = $exp and window = $window. " +
            s"Intended to measure the samplings." +
            s"Date of generation: ${Utils.dateTime}",
          initial = exp,
          (exp, 0.5, window))
    }
  }

  def dataset_11_mid_drifts(): Unit = {
    val currentDataSet = "dataset_11_mid_drifts"

    val combinations: Seq[(Double, Int)] = for {
      exponent <- Seq(0.8D, 1D, 1.25D, 1.5D, 1.75D, 2D, 2.25D, 2.5D, 2.75D, 3D, 3.2D)
      windowSize <- Seq(1, 50000, 100000, 150000, 250000, 400000)
    } yield (exponent, windowSize)

    combinations
      .par
      .foreach {
        case (exp: Double, window: Int) =>
          zetas(currentDataSet,
            short = s"mid_drift_${exp}_$window",
            decs = s"One drift at the middle of the stream with zeta distribution, exponent = $exp and window = $window",
            initial = exp,
            (exp, 0.5, window))
      }
  }


  def dataset_6_base(): Unit = {
    val currentDataSet = "test_dataset_6"

    def e: Double = Utils.randBetween(1.0D, 3.0D)

    def window: Int = Utils.randBetween(100000, 300000)

    zetas(currentDataSet, "Very_Early_Drift_One", "", e, (e, 0.1, window))
    zetas(currentDataSet, "1Early_Drift_One", "", e, (e, 0.3, window))

    zetas(currentDataSet, "1Late_Drift_One", "", e, (e, 0.7, window))
    zetas(currentDataSet, "Very_Late_Drift_One", "", e, (e, 0.9, window))
    zetas(currentDataSet, "Edge_Drift_Two", "", e, (e, 0.1, window), (e, 0.9, window))
    zetas(currentDataSet, "Central_Drift_Two", "", e, (e, 0.3, window), (e, 0.7, window))
    zetas(currentDataSet, "Central_Drift_Three", "", e, (e, 0.3, window), (e, 0.5, window), (e, 0.7, window))
    zetas(currentDataSet, "Spaced_Drift_Three", "", e, (e, 0.25, window), (e, 0.5, window), (e, 0.75, window))
    zetas(currentDataSet, "Drift_A_Ton_Four", "", e, (e, 0.1, window), (e, 0.3, window), (e, 0.6, window), (e, 0.9, window))
  }

  def dataset_10_no_drift(): Unit = {
    val currentDataSet = "test_dataset_10_no_drift"
    (1 to 30) foreach {
      _ =>
        val exp = Utils.round(Utils.randBetween(1.0D, 3.0D), 3)
        zeta(currentDataSet, s"no_drift_e_$exp", "", exp)
    }
  }

  def sufficientlyScrumbled(scramblingA: Array[Int], scramblingB: Array[Int]): Boolean = {
    val top10A = scramblingA.take(30)
    val top100B = scramblingB.take(100)

    val result = top10A.forall(
      x => !top100B.contains(x)
    )

    if (!result) {
      println("Scrumble was actually bad would you look at that chat")
    }
    result
  }

  def zetas(dataset: String, short: String, decs: String, initial: Double, fd: (Double, Double, Int), ds: (Double, Double, Int)*): Unit = {

    val initialDist = getDistribution("zeta", nShops, initial)
    val initialStream = new DistributionShopStreamGenerator(initialDist, rand.nextInt())

    initialStream.restart()
    var lastScrambling: Array[Int] = initialStream.getShopMapping

    val drifters = (Seq(fd) ++ ds).map {
      x =>
        val distribution = getDistribution("zeta", nShops, x._1)
        var distrStreamGenerator: Option[Stream[Shop.Type]] = None

        while (distrStreamGenerator.isEmpty) {
          val d = new DistributionShopStreamGenerator(distribution, rand.nextInt())

          // Note it's safe to restart it here ONLY BECAUSE it is a seeded streamgenerator!
          d.restart()

          val currentScrambling = d.getShopMapping

          if (sufficientlyScrumbled(lastScrambling, currentScrambling)) {
            distrStreamGenerator = Some(d)
            lastScrambling = currentScrambling
          }
        }
        Drifter(distrStreamGenerator.get, x._2, x._3)
    }

    val stream = ConceptDriftStream(
      initialStream,
      drifters.head,
      drifters.tail: _*
    )
    stream.init()
    val generated = Array.fill(nElements) {
      stream.next()
    }

    val title = decs + s"_ Random Stream Parameters_: initialZetaE: $initial, firstDrift: e=${fd._1}, w=${fd._3}, dr=${fd._2}, drifts: [${ds.map(x => s"e=${fd._1}, w=${fd._3}, dr=${fd._2}").mkString(",")}]"

    val config: StreamConfig = StreamConfigFactory(
      title,
      nElements,
      nShops,
      getNewNameFor(dataset, short),
      initialStream,
      drifters: _*)

    Plotter.scatter(title, 2560, 1440, directory + config.fileName, backgroundRunning = true, nElements, generated)

    DStreamWriter.write(config, directory + config.fileName, generated)
  }

  def zeta(dataset: String, short: String, decs: String, initial: Double): Unit = {

    val initialDist = getDistribution("zeta", nShops, initial)
    val stream = new DistributionShopStreamGenerator(initialDist, rand.nextInt())

    stream.init()
    val generated = Array.fill(nElements) {
      stream.next()
    }

    val title = decs + s"_ Random Stream Parameters_: initialZetaE: $initial"

    val config: StreamConfig = StreamConfigFactory(
      title,
      nElements,
      nShops,
      getNewNameFor(dataset, short),
      stream)

    Plotter.scatter(title, 1280, 720, directory + config.fileName, backgroundRunning = true, nElements, generated)

    DStreamWriter.write(config, directory + config.fileName, generated)
  }

  def fullRandom(dataset: String, short: String, decs: String, nDriftsMin: Int, nDriftsMax: Int, nOffsetMin: Double, nOffsetMax: Double, nWindowMin: Int, nWindowMax: Int): Unit = {
    val initialStream = DistributionShopStreamGenerator.random(nShops)
    val firstDrifter = Drifter.random(nShops, nWindowMin, nWindowMax)

    var lastDriftOffset = firstDrifter.offsetPrc

    val drifters = (1 to (Random.nextInt(nDriftsMax) + nDriftsMin))
      .map(_ => {
        val drifter = Drifter.random(nShops, nWindowMin, nWindowMax)
        lastDriftOffset = drifter.offsetPrc
        drifter
      })

    val stream = ConceptDriftStream(
      initialStream,
      firstDrifter,
      drifters: _*
    )
    stream.init()
    val generated = Array.fill(nElements)(stream.next())

    val title = decs + s"_ Random Stream Parameters_: nDriftMIN: $nDriftsMin, nDriftMAX: $nDriftsMax, nWindowMIN: $nWindowMin, nWindowMAX: $nWindowMax"

    val config: StreamConfig = StreamConfigFactory(
      title,
      nElements,
      nShops,
      getNewNameFor(dataset, short),
      initialStream,
      Seq(firstDrifter) ++ drifters: _*)

    Plotter.scatter(title, 1920, 1080, directory + config.fileName, backgroundRunning = true, nElements, generated)

    DStreamWriter.write(config, directory + config.fileName, generated)
  }

  def uzu(dataset: String, short: String, desc: String, d1: Double, d2: Double, window: Int, zE: Double, zS: Double): Unit = {
    val uniformDist = Distribution.uniform(nShops)
    val zetaDist = Distribution.zeta(1, nShops)

    val uniformGen = new DistributionShopStreamGenerator(uniformDist, rand.nextInt())
    val zetaGenerator = new DistributionShopStreamGenerator(zetaDist, rand.nextInt())

    val firstDrifter = Drifter(zetaGenerator, d1, window)
    val secondDrifter = Drifter(uniformGen, d2, window)

    val dataStream: ConceptDriftStream[Shop.Type] = ConceptDriftStream(
      uniformGen,
      firstDrifter,
      secondDrifter)
    dataStream.init()

    val config: StreamConfig = StreamConfigFactory(
      desc,
      nElements,
      nShops,
      getNewNameFor(dataset, short),
      uniformGen,
      Seq(firstDrifter, secondDrifter): _*)

    DStreamWriter.write(config, directory + config.fileName, dataStream)
  }

  val DistributionCache: mutable.Map[String, Distribution] = mutable.Map.empty[String, Distribution]

  def getDistribution(t: String, width: Int, params: Double*): Distribution = synchronized {
    val id = s"${t}_${width}_${params.mkString(",")}"
    if (!DistributionCache.contains(id))
      DistributionCache.put(id, Distribution(t, width, params))

    DistributionCache(id)
  }
}
