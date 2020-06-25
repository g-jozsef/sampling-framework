package hu.elte.inf.sampling.decisioncore

import hu.elte.inf.sampling.core.{DynamicPartitioner, HistogramInfo, Logger}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

abstract class Decider[T](numPartitions: Int) extends Logger {
  val describe: String

  /**
   * versionid - histogram
   */
  val histograms: mutable.Map[Int, HistogramInfo[T]] = mutable.Map.empty
  var currentPartitioner: DynamicPartitioner
  var currentAggregatedHistogram: Option[HistogramInfo[T]] = None
  val balances: ArrayBuffer[Double] = mutable.ArrayBuffer.empty[Double]

  var partitionerDrop: Option[DynamicPartitioner => Unit] = None

  def getNumPartitions: Int = numPartitions

  def getCurrentPartitioner: DynamicPartitioner = currentPartitioner

  def setCurrentPartitioner(partitioner: DynamicPartitioner): Unit = {
    currentPartitioner = partitioner
    broadcastPartitionerChange()
  }

  def onHistogramArrival(partitionId: Int, histogramInfo: HistogramInfo[T]): Unit = {

    if (histograms.contains(histogramInfo.version)) {
      histograms(histogramInfo.version).append(histogramInfo)
    } else {
      histograms.put(histogramInfo.version, histogramInfo)
    }

    if (checkIfNewPartitionerShouldBeCalculated(histograms(histogramInfo.version))) {
      logDebug("Triggering repartitioner")
      repartition()
    } else {
      logDebug("Repartitioning is a no-go")
    }

    if (currentAggregatedHistogram.nonEmpty && currentAggregatedHistogram.get.version != histogramInfo.version) {
      logDebug(s"window = ${histogramInfo.version}, key = ${currentAggregatedHistogram.get.histogram.head}, partiton = ${currentPartitioner.getPartition(currentAggregatedHistogram.get.histogram.head._1)}")
      balances.append(calculateBalanceErrorWithPartitioner(currentPartitioner))
    }

    currentAggregatedHistogram = Some(histograms(histogramInfo.version))
  }

  def checkIfNewPartitionerShouldBeCalculated(histogramInfo: HistogramInfo[T]): Boolean

  def repartition(): Unit = {
    val partitioner = obtainNewParititoner()

    setCurrentPartitioner(partitioner)
  }

  def calculateBalanceErrorWithPartitioner(partitioner: DynamicPartitioner): Double = {
    val normalized = currentAggregatedHistogram.get.normalize

    val partitionHistogram = Array.fill[Double](numPartitions)(0.0d)
    // calculate the partition histogram
    normalized foreach {
      case (key, weight) =>
        partitionHistogram(partitioner.getPartition(key)) += weight
    }

    // We assume there is a sync point at the end of our calculation so the most skewed partition will determine our speed
    val possiblyMostSkewedPartitionWeight = partitionHistogram.max

    // The most optimal balance is a uniform distribution
    val mostOptimalWeight = normalized.map(x => x._2).sum / numPartitions

    // fitness will be how far are we from the perfect balance
    val balance = math.abs(mostOptimalWeight - possiblyMostSkewedPartitionWeight)
    balance
  }

  def obtainNewParititoner(): DynamicPartitioner

  def broadcastPartitionerChange(): Unit
}
