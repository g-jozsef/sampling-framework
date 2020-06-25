package hu.elte.inf.sampling.core

import scala.collection.mutable

case class HistogramInfo[T](var histogram: Array[(T, Long)], version: Int, var mult: Int = 1) {
  // Sorted by weight
  assert(histogram.indices.forall(i => i == 0 || histogram(i - 1)._2 >= histogram(i)._2))

  def truncate(newSize: Int): Unit = {
    if (histogram.length > newSize) {
      histogram = histogram.take(newSize)
    }
  }

  def append(other: HistogramInfo[T]): Unit = {
    assert(version == other.version)
    mult += 1

    val tempMap: mutable.Map[T, Long] = mutable.Map.empty ++ histogram

    other.histogram.foreach(item => {
      if (tempMap.contains(item._1)) {
        tempMap(item._1) += item._2
      } else {
        tempMap(item._1) = item._2
      }
    })

    histogram = tempMap.toArray[(T, Long)].sortBy(-_._2)
  }

  def normalize: Array[(T, Double)] = {
    // Get the sum of all counters
    var sum: Long = 0L
    histogram.foreach((pair: (T, Long)) => sum += pair._2)

    // Normalize each conter with the sum of all counters
    histogram.map(x => (x._1, x._2.toDouble / sum)).filter(x => x._2 > 0)
  }
}
