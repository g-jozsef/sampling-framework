package hu.elte.inf.sampling.datagenerator

import hu.elte.inf.sampling.core.Shop
import hu.elte.inf.sampling.core.TypeDefs.TDriftPrc

import scala.util.Random

case class Drifter[TItem](stream: Stream[TItem],
                          offsetPrc: TDriftPrc,
                          window: Int)(implicit val nElements: Int) {
  assert(offsetPrc >= 0, "OffsetPrc must be >= 0")
  assert(offsetPrc <= 1, "OffsetPrc must be <= 1")
}

object Drifter {
  def random(nShops: Int, windowMin: Int, windowMax: Int)(implicit nElements: Int): Drifter[Shop.Type] =
    Drifter(DistributionShopStreamGenerator.random(nShops), Random.nextDouble(), Random.nextInt(windowMax - windowMin) + windowMin)
}