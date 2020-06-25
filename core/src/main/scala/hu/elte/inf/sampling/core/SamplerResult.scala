package hu.elte.inf.sampling.core

import hu.elte.inf.sampling.core.TypeDefs.{TDriftPrc, TFrequency}

case class SamplerResult[T](drifts: Array[TDriftPrc],
                            relativeFreq: Array[Array[(T, TFrequency)]],
                            directOutput: Array[Array[(T, Long)]])
