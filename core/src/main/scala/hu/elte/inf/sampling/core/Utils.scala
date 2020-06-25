package hu.elte.inf.sampling.core

import java.io
import java.io.FileFilter
import java.text.SimpleDateFormat
import java.util.Calendar

import scala.util.Random

object Utils {
  val rand: Random = new Random()

  def dateTime: String = new SimpleDateFormat("d-M-y-hh-m-s").format(Calendar.getInstance().getTime)


  def addArrays(addTo: Array[Double], add: Array[Double]): Unit = {
    for (i <- addTo.indices) {
      addTo(i) = addTo(i) + add(i)
    }
  }

  def clamp[@specialized(Int, Double, Float) T](value: T,
                                                min: T,
                                                max: T)
                                               (implicit arith: Numeric[T]): T = {
    if (arith.gt(value, max))
      max
    else if (arith.lt(value, min))
      min
    else
      value
  }

  def round(value: Double, precision: Int = 0): Double = {
    val p: Double = math.pow(10, precision)
    (value * p).round.toDouble / p
  }

  def plusMinusX(x: Double): Double = {
    if (rand.nextBoolean()) x else -x
  }

  def plusMinusX(x: Int): Int = {
    if (rand.nextBoolean()) x else -x
  }

  def randBetween(min: Double, max: Double): Double = {
    assert(min < max)
    rand.nextDouble() * (max - min) + min
  }

  def randBetween(min: Int, max: Int): Int = {
    assert(min < max)
    (rand.nextDouble() * (max - min) + min).toInt
  }

  def randBetween[T](min: T, max: T)(implicit arith: Numeric[T]): T = {
    assert(arith.lt(min, max))
    val rr = rand.nextDouble() * arith.toDouble(arith.minus(max, min))
    min match {
      case _: Int =>
        arith.plus(rr.toInt.asInstanceOf[T], min)
      case _: Double =>
        arith.plus(rr.asInstanceOf[T], min)
      case _: Float =>
        arith.plus(rr.toFloat.asInstanceOf[T], min)
      case _: Byte =>
        arith.plus(rr.toByte.asInstanceOf[T], min)
      case _: Long =>
        arith.plus(rr.toLong.asInstanceOf[T], min)
      case _: Short =>
        arith.plus(rr.toShort.asInstanceOf[T], min)
      case _ =>
        throw new NotImplementedError()
    }

  }

  def getFilesFromDirectory(directory: String): Array[String] = {
    new io.File(directory).listFiles(new FileFilter {
      override def accept(pathname: io.File): Boolean = {
        pathname.getPath.contains(".dstream")
      }
    }).map(file => {
      file.getName.replace(".dstream", "")
    })
  }

  def getObjectSimpleName(a: Any): String = {
    a.getClass.getSimpleName.replace("$", "")
  }

  def normalize[T](histogram: Array[(T, Long)]): Array[(T, Double)] = {
    // Get the sum of all counters
    var sum: Long = 0L
    histogram.foreach((pair: (T, Long)) => sum += pair._2)

    // Normalize each conter with the sum of all counters
    histogram.map(x => (x._1, x._2.toDouble / sum)).filter(x => x._2 > 0)
  }

  def combinations[A](xss: List[List[A]]): List[List[A]] = xss match {
    case Nil => List(Nil)
    case xs :: rss => for (x <- xs; cs <- combinations(rss)) yield x :: cs
  }
}
