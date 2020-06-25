package hu.elte.inf.sampling.core

import scala.reflect.ClassTag

trait HyperParameter[+T] {
  def value: T
}

object HyperParameter {

  trait OptimizedHyperParameter[+T] extends HyperParameter[T] {

    def getNeighbours: Iterable[OptimizedHyperParameter[T]]
  }

  class OptimizedNumericHyperParameter[@specialized(Int, Double, Float) +T](val value: T,
                                                                            val minValue: T,
                                                                            val maxValue: T,
                                                                            val step: T)
                                                                           (implicit arith: Numeric[T])
    extends OptimizedHyperParameter[T] {
    type ParamType

    assert(arith.gteq(value, minValue) && arith.lteq(value, maxValue),
      s"Numeric Hyperparameter is out of bounds [$minValue, $maxValue], with value of $value")

    override def getNeighbours: Iterable[OptimizedNumericHyperParameter[T]] = {
      Seq(

        new OptimizedNumericHyperParameter(Utils.clamp(arith.plus(value, step), minValue, maxValue),
          minValue,
          maxValue,
          step),

        new OptimizedNumericHyperParameter(Utils.clamp(arith.minus(value, step), minValue, maxValue),
          minValue,
          maxValue,
          step)

      )
    }

    def mutate: OptimizedNumericHyperParameter[T] = {
      new OptimizedNumericHyperParameter(Utils.randBetween(minValue, maxValue),
        minValue,
        maxValue,
        step)

    }
  }

  type Boundary = IntegerParam
  type TopK = IntegerParam
  type Window = IntegerParam
  type Threshold = IntegerParam
  type DThreshold = DoubleParam
  type EncapsulatedAlgorithm[+S <: Sampling, C <: AlgorithmConfiguration] = EncapsulatedAlgorithmParam[S, C]

  case class EncapsulatedAlgorithmParam[+S <: Sampling : ClassTag, C <: AlgorithmConfiguration : ClassTag](value: SamplerCase[S, C])
    extends HyperParameter[SamplerCase[S, C]]

  case class DoubleParam(override val value: Double,
                         override val minValue: Double = 0,
                         override val maxValue: Double = 1,
                         override val step: Double = 0.0001)(implicit arith: Numeric[Double])
    extends OptimizedNumericHyperParameter[Double](value, minValue, maxValue, step)

  case class IntegerParam(override val value: Int,
                          override val minValue: Int = 1,
                          override val maxValue: Int = 100000,
                          override val step: Int = 1000)(implicit arith: Numeric[Int])
    extends OptimizedNumericHyperParameter[Int](value, minValue, maxValue, step)


  case class Probability(override val value: Double,
                         override val minValue: Double = 0.000001,
                         override val maxValue: Double = 0.999999,
                         override val step: Double = 0.0001)(implicit arith: Numeric[Double])
    extends OptimizedNumericHyperParameter[Double](value, minValue, maxValue, step)

}