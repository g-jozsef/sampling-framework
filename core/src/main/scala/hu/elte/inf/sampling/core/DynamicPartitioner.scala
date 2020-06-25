package hu.elte.inf.sampling.core

import scala.util.Random

/**
 * An updateable partitioner.
 * Such a partitioner's internal state can be updated after each batch of new data, in order to
 * adapt partitioning to changes in key distribution.
 */
trait DynamicPartitioner extends Partitioner {

  val id: String = {
    val idLength: Int = 10
    val alphaNumList: Seq[Char] =
      ((48 to 57) ++ (65 to 90) ++ (95 to 95) ++ (97 to 122)).map(_.toChar)

    (1 to idLength).map(_ => {
      val hashed = Random.nextInt(alphaNumList.size)
      alphaNumList(hashed)
    }).mkString
  }

  def createUpdated(partitioningInfo: PartitioningInfo): DynamicPartitioner

  override def hashCode(): Int = id.hashCode()

  override def equals(other: Any): Boolean = other match {
    case p: DynamicPartitioner => p.id == id
    case _ => false
  }

  override def toString: String = s"Partitioner($id)"
}