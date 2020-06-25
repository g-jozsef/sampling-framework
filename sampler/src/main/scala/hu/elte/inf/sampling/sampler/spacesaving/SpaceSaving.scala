/**
 * The MIT License (MIT)
 * Copyright (c) 2016 Farid Zakaria <farid.m.zakaria@gmail.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package hu.elte.inf.sampling.sampler.spacesaving

import java.util.concurrent.atomic.LongAdder

import hu.elte.inf.sampling.core.DoubleLinkedList.Node
import hu.elte.inf.sampling.core._
import hu.elte.inf.sampling.sampler.spacesaving.SpaceSaving.{Bucket, Counter}

import scala.collection.mutable


class SpaceSaving(config: AlgorithmConfiguration)
  extends SamplingBase(config) {

  private val epsilon: Double = config.getParam[Double]("epsilon")

  val capacity = (1.0 / epsilon).ceil.toInt
  val buckets = new DoubleLinkedList[Bucket[Any]]()

  val newBucket = new Bucket[Any](0)
  val node = new Node[Bucket[Any]](newBucket)
  1 to capacity foreach {
    _ =>
      newBucket.contents += new Counter[Any](node)
  }
  buckets.insertBeginning(node)

  private val n = new LongAdder()
  private val counterCache = new mutable.HashMap[Any, Counter[Any]]()

  private def incrementCounter(counter: Counter[Any]): Unit = {
    val bucketNode = counter.bucket
    val bucketNodeNext = bucketNode.next

    counter.value += 1

    val aloneInBucket = bucketNode.getItem.contents.size == 1

    if (aloneInBucket) {
      if (bucketNodeNext != null && counter.value == bucketNodeNext.getItem.value) {
        buckets.remove(bucketNode);
        bucketNodeNext.getItem.contents.add(counter)
        counter.bucket = bucketNodeNext
      } else {
        bucketNode.getItem.value = counter.value
      }
    } else {
      bucketNode.getItem.contents.remove(counter)
      if (bucketNodeNext != null && counter.value == bucketNodeNext.getItem.value) {
        bucketNodeNext.getItem.contents.add(counter)
        counter.bucket = bucketNodeNext
      } else {
        CreateNodeAfter(bucketNode, counter)
      }
    }
  }

  private def CreateNodeAfter(node: Node[Bucket[Any]], counter: Counter[Any]): Unit = {
    val newNode = new Node[Bucket[Any]](new Bucket[Any](counter.value))
    newNode.getItem.contents.add(counter)
    buckets.insertAfter(node, newNode)
    counter.bucket = newNode
  }

  /**
   * Over the lifetime of this sampler how much elements did it process
   */
  override def getTotalProcessedElements: Long = n.longValue()

  /**
   * Memory usage in bytes
   */
  override def estimateMemoryUsage: Int = counterCache.size * 2

  /**
   * Process a data entry with a given key
   */
  override def recordKey(key: Any, multiplicity: Int): Unit = {
    var i = 0
    while (i < multiplicity) {
      n.increment()

      incrementCounter(
        counterCache.get(key) match {
          case Some(counter) =>
            counter
          case None =>
            val smallestBucket = buckets.getHead.getItem

            // Doesn't matter which all have same value
            val minElement = smallestBucket.contents.head

            val originalMinValue = minElement.value

            minElement.key
              .map(counter =>
                counterCache.remove(counter))
              .foreach(_ =>
                minElement.error = originalMinValue)

            counterCache.put(key, minElement)
            minElement.key = Some(key)

            minElement
        }
      )
      i += 1
    }
  }

  /**
   * Returns an estimate of the frequencies
   */
  override def estimateProcessedKeyNumbers(): mutable.Map[Any, Long] = {
    mutable.Map.empty[Any, Long] ++ (buckets
      .flatMap(bucket => bucket.contents)
      .filter(counter => counter.key.isDefined)
      .map(counter => (counter.key.get, (counter.value - counter.error))))
  }
}

object SpaceSaving {

  /**
   * @param epsilon (1/epsilon).ceil is the maximum number of unique counters in memory
   */
  case class Config(epsilon: Double)
    extends AlgorithmConfiguration("SpaceSaving",
      "epsilon" -> HyperParameter.Probability(epsilon))

  class Counter[T](var bucket: Node[Bucket[T]]) extends Serializable {
    var value = 0L
    var error = 0L
    var key: Option[T] = None
  }

  class Bucket[T](var value: Long) extends Serializable {
    val contents = mutable.HashSet.empty[Counter[T]]
  }

}
