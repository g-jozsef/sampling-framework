package hu.elte.inf.sampling.decisioncore

import hu.elte.inf.sampling.core.ErrorFunction.KullbackLeiblerDivergence
import hu.elte.inf.sampling.core.TypeDefs.TErrorPrecision
import hu.elte.inf.sampling.core._

class TestDecider[T, C <: PartitionerConfig](partitionerFactory: PartitionerFactory[C],
                                             partitionerConfig: C,
                                             numPartitions: Int,
                                             topK: Int,
                                             keyExcess: Int,
                                             treshold: TErrorPrecision) extends Decider[T](numPartitions) with Logger {

  override val describe: String = s"TestDecider-${Utils.getObjectSimpleName(partitionerFactory)}, np=${numPartitions}"
  override var currentPartitioner: DynamicPartitioner = partitionerFactory(numPartitions, partitionerConfig)
  private val treeDepthHint = (Math.log10(numPartitions) / Math.log10(2.0d)).toInt + 2

  protected val kullback: KullbackLeiblerDivergence[T] = new KullbackLeiblerDivergence[T]()
  protected var expected: Array[(T, TErrorPrecision)] = _

  protected var lastN: Seq[TErrorPrecision] = Seq()
  protected var count: Int = 0

  override def checkIfNewPartitionerShouldBeCalculated(histogramInfo: HistogramInfo[T]): Boolean = {
    if (currentAggregatedHistogram.nonEmpty) {
      val actual: Array[(T, TErrorPrecision)] = Utils.normalize(currentAggregatedHistogram.get.histogram.take(topK))
      var doRepartition: Boolean = false
      if (expected != null) {
        val (smoothedExp, smoothedAct) = kullback.smooth(expected, actual)
        val result: TErrorPrecision = kullback.dkl(smoothedExp, smoothedAct)

        doRepartition = result > treshold

        logInfo("result: " + result.toString)

        if (doRepartition) {
          logInfo("change!")
          expected = actual
        }

      } else {
        expected = actual
      }

      doRepartition
    } else {
      false
    }
  }

  override def obtainNewParititoner(): DynamicPartitioner = {
    if (histograms.nonEmpty) {

      val globalHistogram: scala.collection.Seq[(T, Double)] = currentAggregatedHistogram.get.normalize

      if (globalHistogram.nonEmpty) {
        currentPartitioner.createUpdated(PartitioningInfo(globalHistogram.take(keyExcess * numPartitions).toMap, numPartitions, treeDepthHint))
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
