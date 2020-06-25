package hu.elte.inf.sampling.decisioncore

import hu.elte.inf.sampling.core._

/**
 * No Operation
 */
class NoOpDecider[T, C <: PartitionerConfig](partitionerFactory: PartitionerFactory[C],
                                             partitionerConfig: C,
                                             numPartitions: Int) extends Decider[T](numPartitions) with Logger {

  override val describe: String = s"NoOpDecider-${Utils.getObjectSimpleName(partitionerFactory)}, np=${numPartitions}"
  override var currentPartitioner: DynamicPartitioner = partitionerFactory(numPartitions, partitionerConfig)

  override def obtainNewParititoner(): DynamicPartitioner = {
    currentPartitioner
  }

  override def broadcastPartitionerChange(): Unit = {
  }

  override def checkIfNewPartitionerShouldBeCalculated(histogramInfo: HistogramInfo[T]): Boolean = {
    false
  }
}
