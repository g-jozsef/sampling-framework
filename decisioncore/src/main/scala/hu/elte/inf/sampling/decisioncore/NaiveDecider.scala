package hu.elte.inf.sampling.decisioncore

import hu.elte.inf.sampling.core._

class NaiveDecider[T, C <: PartitionerConfig](partitionerFactory: PartitionerFactory[C],
                                              partitionerConfig: C,
                                              keyExcess: Int) extends Decider[T](partitionerConfig.numPartitions) with Logger {

  override val describe: String = s"NaiveDecider-${Utils.getObjectSimpleName(partitionerFactory)}, np=${partitionerConfig.numPartitions}"
  override var currentPartitioner: DynamicPartitioner = partitionerFactory(partitionerConfig.numPartitions, partitionerConfig)
  private val treeDepthHint = (Math.log10(partitionerConfig.numPartitions) / Math.log10(2.0d)).toInt + 2

  override def checkIfNewPartitionerShouldBeCalculated(histogramInfo: HistogramInfo[T]): Boolean = {
    currentAggregatedHistogram.nonEmpty && currentAggregatedHistogram.get.version != histogramInfo.version
  }

  override def obtainNewParititoner(): DynamicPartitioner = {
    if (histograms.nonEmpty) {

      val globalHistogram: scala.collection.Seq[(T, Double)] = currentAggregatedHistogram.get.normalize

      if (globalHistogram.nonEmpty) {
        currentPartitioner.createUpdated(PartitioningInfo(globalHistogram.take(keyExcess * partitionerConfig.numPartitions).toMap, partitionerConfig.numPartitions, treeDepthHint))
      } else {
        logError("Empty global histogram how could this happen here?")

        currentPartitioner
      }
    } else {
      currentPartitioner
    }
  }

  override def broadcastPartitionerChange(): Unit = {
    partitionerDrop match {
      case Some(drop) =>
        drop(getCurrentPartitioner)
      case None =>
        logDebug("PartitionerDrop not enabled!")
    }
  }
}
