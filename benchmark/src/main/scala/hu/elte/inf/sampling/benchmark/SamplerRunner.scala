package hu.elte.inf.sampling.benchmark

import hu.elte.inf.sampling.benchmark.tools.{InMemoryDataStream, Runner}
import hu.elte.inf.sampling.core.{AlgorithmConfiguration, Logger, SamplerCase, Sampling}

import scala.reflect.ClassTag

class SamplerRunner[T: ClassTag, R](topK: Int,
                                    testCase: SamplerCase[Sampling, AlgorithmConfiguration],
                                    streamMaker: StreamMaker[T],
                                    runner: Runner[T, R, Sampling, AlgorithmConfiguration, SamplerCase[Sampling, AlgorithmConfiguration]])
  extends Logger {

  logDebug(s"GENERATING STREAM...")
  protected val stream: InMemoryDataStream[T] = streamMaker.makeStream()

  def run(): R = {
    logDebug(s"Running sampler for ${testCase.toString}...")
    runner.run(topK, stream, testCase)
  }
}
