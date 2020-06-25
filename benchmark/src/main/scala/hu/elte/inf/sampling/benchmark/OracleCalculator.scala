package hu.elte.inf.sampling.benchmark

import hu.elte.inf.sampling.core.{HistogramInfo, Logger, SamplerResult, StreamConfig}
import hu.elte.inf.sampling.sampler.oracle.Oracle
import hu.elte.inf.sampling.sampler.oracle.Oracle.TFactory

import scala.collection.mutable.ArrayBuffer

trait OracleCalculator[T] {
  def calculateOracle(intendedWindowSize: Int,
                      topK: Int,
                      meta: StreamConfig): SamplerResult[T]
}

object OracleCalculator {

  class NormalOracleCalulator[T](factory: TFactory[T]) extends OracleCalculator[T] with Logger {
    def calculateOracle(intendedWindowSize: Int,
                        topK: Int,
                        meta: StreamConfig): SamplerResult[T] = {

      val oracle = new Oracle(Oracle.Config[T](intendedWindowSize, meta, topK, factory))

      val windowCount = meta.nElements / intendedWindowSize

      val relativeFrequencies = new Array[Array[(T, Double)]](windowCount)
      val directOutput = new Array[Array[(T, Long)]](windowCount)
      val drifts = new ArrayBuffer[Int](0)
      var lastTop1: Option[T] = None

      var startTime = System.nanoTime()

      var windowIndex: Int = 1
      while (windowIndex <= windowCount) {
        val output = HistogramInfo(oracle.computeOutput(windowIndex * intendedWindowSize).sortBy(-_._2), windowIndex)

        val processingStartTime = System.nanoTime()

        if (topK > 0)
          output.truncate(topK)

        if (output.histogram.length > 0) {
          val top1 = output.histogram(0)._1
          lastTop1 match {
            case Some(value) =>
              if (!value.equals(top1)) {
                lastTop1 = Some(top1)
                drifts.append(windowIndex - 1)
              }
            case None =>
              lastTop1 = Some(top1)
          }
        }

        // Normalize each conter with the sum of all counters
        val relFreq = output.normalize
        if (relFreq.isEmpty) {
          logError("relative frequency was empty for oracle calculation")
        }

        relativeFrequencies(windowIndex - 1) = relFreq
        directOutput(windowIndex - 1) = output.histogram

        windowIndex += 1
        startTime = startTime + (System.nanoTime() - processingStartTime)
      }

      val driftsNormalized = drifts.map(x => x.toDouble / (windowCount)).toArray

      new SamplerResult[T](driftsNormalized, relativeFrequencies, directOutput)
    }
  }

}
