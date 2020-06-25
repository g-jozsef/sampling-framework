package hu.elte.inf.sampling.datagenerator

import hu.elte.inf.sampling.core.{Distribution, Shop}

import scala.util.Random

class DistributionShopStreamGenerator(val dist: Distribution,
                                      seed: Int = 0)
  extends StreamGenerator[Shop.Type](seed) {

  private var shopMapping: Option[Array[Int]] = None

  def getShopMapping: Array[Int] = shopMapping.orNull

  override def next(): Shop.Type = {
    val sample = dist.sample()
    val shopId = shopMapping.get(sample - 1)
    shopId
  }

  override def restart(): Unit = {
    super.restart()
    shopMapping = Some(rand.shuffle((1 to dist.width).toList).toArray)
  }
}

object DistributionShopStreamGenerator {
  val rand = new Random()

  def random(nShops: Int): DistributionShopStreamGenerator = {
    val distribution: Distribution =
      Distribution.zeta(Random.nextDouble() + 0.4, nShops)

    new DistributionShopStreamGenerator(distribution, rand.nextInt())
  }
}