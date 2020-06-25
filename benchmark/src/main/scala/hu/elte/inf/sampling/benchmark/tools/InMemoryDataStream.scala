package hu.elte.inf.sampling.benchmark.tools

import hu.elte.inf.sampling.core.{SamplerResult, StreamConfig}

case class InMemoryDataStream[T](meta: StreamConfig,
                                 oracle: SamplerResult[T],
                                 microbatches: Array[Int],
                                 data: Array[T])
