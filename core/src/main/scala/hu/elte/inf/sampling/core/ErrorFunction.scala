package hu.elte.inf.sampling.core

import hu.elte.inf.sampling.core.TypeDefs.{TErrorPrecision, TFrequency}

import scala.collection.mutable

trait ErrorFunction[T] {
  type RawOutput = Array[Array[(T, Long)]]
  type Frequencies = Array[Array[(T, TFrequency)]]

  def calculateError(expectedResult: SamplerResult[T], actualResult: SamplerResult[T]): Array[TErrorPrecision]

  protected def getRawOutput(result: SamplerResult[T]): RawOutput = {
    result.directOutput
  }

  protected def getRelFrequencies(result: SamplerResult[T]): Frequencies = {
    result.relativeFreq
  }
}

object ErrorFunction extends Logger {

  /**
   * http://hanj.cs.illinois.edu/cs412/bk3/KL-divergence.pdf
   *
   */
  class KullbackLeiblerDivergence[T] extends ErrorFunction[T] {
    type DoubleDistribution = Array[(T, Double)]
    type BigDecimalDistribution = mutable.Map[T, BigDecimal]

    override def calculateError(expectedResult: SamplerResult[T], actualResult: SamplerResult[T]): Array[TErrorPrecision] = {
      val expected = getRelFrequencies(expectedResult)
      val actual = getRelFrequencies(actualResult)

      val result = new Array[TErrorPrecision](expected.length)

      result.indices.foreach(
        window =>
          if (actual(window).isEmpty)
            result(window) = 40.0
          else {
            val (smoothedExp, smoothedAct) = smooth(expected(window), actual(window))
            result(window) = dkl(smoothedExp, smoothedAct)
          }
      )

      result
    }

    /**
     * Adds the missing symbols in p from q to p
     */
    def smooth(p: DoubleDistribution, q: DoubleDistribution): (BigDecimalDistribution, BigDecimalDistribution) = {
      val pp: mutable.Map[T, BigDecimal] = mutable.Map.empty
      val qp: mutable.Map[T, BigDecimal] = mutable.Map.empty

      p.foreach(pair => pp(pair._1) = BigDecimal(pair._2))
      q.foreach(pair => qp(pair._1) = BigDecimal(pair._2))

      val minItemFreq: BigDecimal = {
        val pMin: Option[BigDecimal] = pp.values.reduceOption(_ min _)
        val qMin: Option[BigDecimal] = qp.values.reduceOption(_ min _)

        if (pMin.isEmpty) {
          logError("??? What to do when this is empty, how can this be empty? Oracle can't be empty!")
          throw new NotImplementedError("??? What to do when this is empty, how can this be empty")
        }

        if (qMin.isEmpty) {
          logWarning("Empty reported")
          pMin.get
        } else {
          pMin.get.min(qMin.get)
        }
      }

      val shopSet = pp.keySet ++ qp.keySet
      val distinctShopCount: Int = shopSet.size

      val pMissingNumber: Int = distinctShopCount - p.length
      val qMissingNumber: Int = distinctShopCount - q.length

      if (pMissingNumber > 0 || qMissingNumber > 0) {
        val epsilon: BigDecimal = minItemFreq / (BigDecimal(2) * pMissingNumber.max(qMissingNumber)) - Double.MinPositiveValue
        assert(epsilon > BigDecimal(0.0D))

        // Go trough all shops
        shopSet.foreach {
          shop: T =>
            // Update elements in p
            if (pMissingNumber > 0 && pp.contains(shop)) {
              pp(shop) = pp(shop) - epsilon * pMissingNumber / BigDecimal(p.length)
              assert(pp(shop) > 0) // [KullbackLeiblerDivergence] If you see this error idk, you cant see this error
            }

            // Update elements in q
            if (qMissingNumber > 0 && qp.contains(shop)) {
              qp(shop) = qp(shop) - epsilon * qMissingNumber / BigDecimal(q.length)
              assert(qp(shop) > 0) // [KullbackLeiblerDivergence] If you see this error idk, you cant see this error
            }

            // Add missing element to p
            if (!pp.contains(shop))
              pp.put(shop, epsilon)

            // Add missing element to q
            if (!qp.contains(shop))
              qp.put(shop, epsilon)
        }
      }
      assert(math.abs(pp.values.sum.doubleValue - 1.0D) < 0.0000000001D)
      assert(math.abs(qp.values.sum.doubleValue - 1.0D) < 0.0000000001D)
      (pp, qp)
    }

    /**
     * Difference between two probability distributionsp(x) andq(x).
     * Specifically, the Kullback-Leibler (KL) divergence ofq(x) fromp(x), denotedDKL(p(x), q(x)),
     * is a measure of the information lost whenq(x) is used to ap-proximatep(x)
     */
    def dkl(p: BigDecimalDistribution, q: BigDecimalDistribution): Double = {
      var sum = 0d
      p foreach {
        x =>
          val qx: BigDecimal = q(x._1)
          if (qx == 0) {
            logError("wtf")
          }
          sum = sum + (x._2.doubleValue * (math.log((qx / x._2).doubleValue) / math.log(2)))
      }
      -sum
    }
  }

  class HellingerDistance[T] extends ErrorFunction[T] {
    override def calculateError(expectedResult: SamplerResult[T], actualResult: SamplerResult[T]): Array[TErrorPrecision] = {
      val expected = getRelFrequencies(expectedResult)
      val actual = getRelFrequencies(actualResult)

      val result = new Array[TErrorPrecision](expected.length)

      result.indices.foreach(
        window =>
          if (actual(window).isEmpty)
            result(window) = 1d
          else {
            result(window) = hd(expected(window).toMap, actual(window).toMap)
          }
      )

      result
    }

    def hd(p: Map[T, TFrequency], q: Map[T, TFrequency]): Double = {
      var sum = 0d
      val keySet = p.keySet ++ q.keySet
      keySet foreach {
        key =>
          val diff = math.pow(math.sqrt(p.getOrElse(key, 0)) - math.sqrt(q.getOrElse(key, 0)), 2)
          sum += diff
      }
      math.sqrt(sum) * (1 / math.sqrt(2))
    }
  }

}
