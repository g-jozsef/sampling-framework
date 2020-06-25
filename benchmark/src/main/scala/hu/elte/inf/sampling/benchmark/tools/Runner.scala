package hu.elte.inf.sampling.benchmark.tools

import hu.elte.inf.sampling.benchmark.TimeBenchmarkResult
import hu.elte.inf.sampling.core
import hu.elte.inf.sampling.core.{SamplerCase, _}
import hu.elte.inf.sampling.decisioncore.{Decider, HashPartitioner}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

trait Runner[T, R, +A, +C, TC <: TestCase[A, C]] {
  def run(topK: Int,
          data: InMemoryDataStream[T],
          testCase: TC): R
}

object Runner extends Logger {

  class BenchmarkerRunner[T: ClassTag : Ordering]()
    extends Runner[T, (TimeBenchmarkResult, SamplerResult[T]), Sampling, AlgorithmConfiguration, SamplerCase[Sampling, AlgorithmConfiguration]] {
    /**
     * Calculates the sampler on a data with a topK (saves topK)
     * topK = 0, if you want to save all
     */
    override def run(topK: Int,
                     str: InMemoryDataStream[T],
                     testCase: SamplerCase[Sampling, AlgorithmConfiguration]):
    (TimeBenchmarkResult, SamplerResult[T]) = {

      val windowCount = str.microbatches.length

      val samplers = Array.fill(testCase.getNumPartition)(testCase.make)
      val partitioner = new HashPartitioner(testCase.getNumPartition)

      val counters: Array[Int] = new Array(windowCount)
      val relativeFrequencies = new Array[Array[(T, Double)]](windowCount)
      val directOutput = new Array[Array[(T, Long)]](windowCount)
      val drifts = new ArrayBuffer[Int](0)
      var lastTop1: Option[T] = None


      var startTime = System.nanoTime()

      var runningDataIndex: Int = 0
      var index: Int = 0
      var windowIndex: Int = 0
      while (windowIndex < windowCount) {
        val windowSize = str.microbatches(windowIndex)
        val (output, counterCount) = if (testCase.getNumPartition == 1) {
          // Process window
          index = 0

          while (index < windowSize) {
            samplers.head.recordKey(str.data(runningDataIndex))
            runningDataIndex += 1
            index += 1
          }
          (HistogramInfo(samplers.head.estimateProcessedKeyNumbers().toArray.asInstanceOf[Array[(T, Long)]].sortBy(-_._2), windowIndex), samplers.head.estimateMemoryUsage)
        } else {
          // Process window
          index = 0

          while (index < windowSize) {
            val currentData = str.data(runningDataIndex)
            samplers(partitioner.getPartition(currentData)).recordKey(currentData)
            runningDataIndex += 1
            index += 1
          }

          // Aggregate window results
          var sumCounter = 0
          val histogramMap = mutable.Map.empty[T, Long]

          samplers.foreach {
            sampler =>
              val computedOutput = sampler.estimateProcessedKeyNumbers().toArray.asInstanceOf[Array[(T, Long)]]

              index = 0
              while (index < computedOutput.length) {
                val item = computedOutput(index)

                if (histogramMap.contains(item._1))
                  histogramMap(item._1) += item._2
                else
                  histogramMap(item._1) = item._2

                index += 1
              }
              sumCounter += sampler.estimateMemoryUsage
          }

          val histogramInfo = new HistogramInfo[T](histogramMap.toArray.sortBy(-_._2), 1)
          (histogramInfo, (sumCounter.toDouble / samplers.length.toDouble).round.toInt)
        }
        val processingStartTime = System.nanoTime()

        counters(windowIndex) = counterCount

        if (topK > 0)
          output.truncate(topK)

        if (output.histogram.length > 0) {
          val top1 = output.histogram(0)._1
          lastTop1 match {
            case Some(value) =>
              if (!value.equals(top1)) {
                lastTop1 = Some(top1)
                drifts.append(windowIndex)
              }
            case None =>
              lastTop1 = Some(top1)
          }
        }

        // Normalize each counter with the sum of all counters
        val relFreq = output.normalize
        if (relFreq.isEmpty) {
          logDebug("relative frequency was empty")
        }

        relativeFrequencies(windowIndex) = relFreq
        directOutput(windowIndex) = output.histogram

        windowIndex += 1
        startTime = startTime + (System.nanoTime() - processingStartTime)
      }

      val timeTaken: Long = System.nanoTime() - startTime

      val driftsNormalized = drifts.map(x => x.toDouble / (windowCount)).toArray

      (TimeBenchmarkResult(counters, timeTaken / 1000000.0D), core.SamplerResult(driftsNormalized, relativeFrequencies, directOutput))
    }
  }

  class DeciderRunner[T: ClassTag](decider: Decider[Any])
    extends Runner[T, Array[Double], Sampling, AlgorithmConfiguration, SamplerCase[Sampling, AlgorithmConfiguration]] {

    override def run(topK: Int,
                     str: InMemoryDataStream[T],
                     testCase: SamplerCase[Sampling, AlgorithmConfiguration]):
    Array[Double] = {
      val windowCount = str.microbatches.length

      val samplers = Array.fill(decider.getNumPartitions)(testCase.make)
      val errors = new Array[Double](windowCount)
      val simplePartitioner = new HashPartitioner(decider.getNumPartitions)

      var runningDataIndex: Int = 0

      var windowIndex: Int = 0
      while (windowIndex < windowCount) {
        val windowSize = str.microbatches(windowIndex)
        // Process window
        var index: Int = 0

        val partitionerForThisMicroBatch = decider.getCurrentPartitioner

        val partitions = Array.fill[Long](decider.getNumPartitions)(0)

        while (index < windowSize) {
          val currentData = str.data(runningDataIndex)

          samplers(simplePartitioner.getPartition(currentData)).recordKey(currentData)

          partitions(partitionerForThisMicroBatch.getPartition(currentData)) += 1

          runningDataIndex += 1
          index += 1
        }

        val numberOfElements: Double = windowSize.toDouble
        val perfectLoad = numberOfElements / decider.getNumPartitions

        val mostLoadedPartition: Double = partitions.max.toDouble
        val imbalance = mostLoadedPartition / perfectLoad - 1

        assert(imbalance >= 0 && imbalance <= decider.getNumPartitions - 1)

        errors(windowIndex) = imbalance

        // Process sampler histograms
        index = 0
        while (index < samplers.length) {
          decider.onHistogramArrival(index,
            new HistogramInfo[Any](samplers(index).estimateProcessedKeyNumbers()
              .toArray
              .sortBy(-_._2), windowIndex)
          )
          index += 1
        }

        windowIndex += 1
      }

      errors
    }
  }

}
