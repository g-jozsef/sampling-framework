package hu.elte.inf.sampling.core


/**
 * Informations required to construct the partitioner
 *
 * @param partitions         number of partitions
 * @param cut                UNUSED
 * @param sCut               UNUSED
 * @param level              optimal minimal size of partitions;
 *                           the partitioner will try to fill up every
 *                           partition with lightweight keys up to this level
 * @param heavyKeys          sequence of heavy keys
 * @param partitionHistogram distribution of partition sizes in the previous batch; not all
 *                           partitioner needs this information
 */
class PartitioningInfo(val partitions: Int,
                       val cut: Int,
                       val sCut: Int,
                       val level: Double,
                       val heavyKeys: Map[Any, Double],
                       val partitionHistogram: Option[Map[Int, Double]] = None)
  extends Serializable {
  lazy val maxEntry: (Any, Double) = heavyKeys.maxBy(x => x._2)
  lazy val sortedValues: Array[Double] = heavyKeys.values.toArray.sortBy(x => -x)

  override def toString: String = {
    s"PartitioningInfo [numberOfPartitions=$partitions, cut=$cut, sCut=$sCut, " +
      s"level=$level, heavyKeys = ${heavyKeys.take(10).mkString("[", ", ", "...]")}]"
  }
}

// Create PartitioningInfo from global key histogram
object PartitioningInfo {
  def apply(globalHistogram: Map[Any, Double],
            numPartitions: Int,
            treeDepthHint: Int,
            sCutHint: Int = 0,
            partitionHistogram: Option[Map[Int, Double]] = None): PartitioningInfo = {
    require(numPartitions > 0, s"Number of partitions ($numPartitions) should be positive.")

    val sortedValues = globalHistogram.toArray.sortBy(-_._2).map(x => x._2).take(numPartitions)
    val pCutHint = Math.pow(2, treeDepthHint - 1).toInt
    val startingCut = Math.min(numPartitions, sortedValues.length)
    var remainder = 1.0d
    var computedLevel = remainder / numPartitions

    val computedSCut = ((0 until startingCut) zip sortedValues).takeWhile({
      case (i: Int, sortedValue: Double) =>
        if (computedLevel <= sortedValue) {
          remainder -= sortedValue
          computedLevel =
            if (i < numPartitions - 1) {
              remainder / (numPartitions - 1 - i)
            } else {
              0.0d
            }
        }
        computedLevel <= sortedValue
    }).length

    val actualSCut = Math.max(sCutHint, computedSCut)
    val actualPCut = Math.min(pCutHint, startingCut - actualSCut)

    /**
     * Recompute level to minimize rounding errors.
     */
    val level =
      Math.max(0, (1.0d - sortedValues.take(actualSCut).sum) / (numPartitions - actualSCut))

    val actualCut = actualSCut + actualPCut
    new PartitioningInfo(
      numPartitions,
      actualCut,
      actualSCut,
      level,
      globalHistogram,
      partitionHistogram)
  }
}
