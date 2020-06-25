package hu.elte.inf.sampling.datagenerator

import java.util.concurrent.atomic.AtomicLong

/**
 *
 * @param window      After how many elements will we use the next generator after we started the transition
 * @param offset      After how many elements will we start transitioning to the next generator
 * @param stream      Stream to be used as the base stream
 * @param driftStream Stream to be used to transition to after offset + window generated elements
 */
class ConceptDriftStream[TItem](val window: Int,
                                val offset: Int,
                                val stream: Stream[TItem],
                                val driftStream: Stream[TItem])
  extends StreamGenerator[TItem] {

  protected val driftStart: Int = offset - (window / 2)
  protected val driftEnd: Int = offset + (window / 2)
  protected val state: AtomicLong = new AtomicLong(0L)

  /**
   * Generates the next shop based on stream generation with concept drift
   *
   * @return Next generated item
   */
  override def next(): TItem = {
    val t = state.incrementAndGet()

    // If drift didn't start yet
    if (t < driftStart)
      stream.next()
    // If drift ended we can skip calculations and just sample from drifting stream
    else if (t > driftEnd)
      driftStream.next()
    // If we are in the middle of a drift, calculate probabilities
    else {
      // Calculate a linear probability shift
      // pDrift will be the probability that we sample from the drifting stream
      // It goes 0 -> 1
      val pDrift: Double = (t - driftStart).toDouble / window

      // If uniform sample is in pDrift, sample from drifting stream
      if (rand.nextDouble() < pDrift)
        driftStream.next()
      // If uniform sample is in pCurrent = (1-pDrift), sample from current stream
      else
        stream.next()
    }
  }

  override def restart(): Unit = {
    super.restart()
    stream.restart()
    driftStream.restart()
    state.set(0)
  }

}

object ConceptDriftStream {
  /**
   * Make a new [[ConceptDriftStream]] from a stream and a drifter
   *
   * @param stream  : Base stream to start drifting away from
   * @param drifter : Drifter to transition towards
   * @return A new [[ConceptDriftStream]] that drifts from a base stream to a target stream
   */
  def apply2[TItem](stream: Stream[TItem], drifter: Drifter[TItem]): ConceptDriftStream[TItem] = {
    new ConceptDriftStream[TItem](drifter.window,
      (drifter.offsetPrc * drifter.nElements).toInt,
      stream,
      drifter.stream)
  }

  /**
   * Make a new [[ConceptDriftStream]] from an arbitrary number of drifters
   *
   * @param base           : BaseStream to apply the rest to
   * @param secondStream   : Drifter to transition towards
   * @param streamsToDrift : Rest od the drifter streams to transition to
   * @return A new [[ConceptDriftStream]] that drifts from a base stream to the last target stream going through all the given streams
   */
  def apply[TItem](base: Stream[TItem], secondStream: Drifter[TItem], streamsToDrift: Drifter[TItem]*): ConceptDriftStream[TItem] = {
    val first: ConceptDriftStream[TItem] = ConceptDriftStream.apply2(base, secondStream)

    streamsToDrift.foldLeft(first)((a, b) => driftingStreamHelper[TItem](a, b)._1)
  }

  def driftingStreamHelper[TItem](d1: Stream[TItem], d2: Drifter[TItem]): (ConceptDriftStream[TItem], Int, Int) = {
    (ConceptDriftStream.apply2(d1, d2), (d2.offsetPrc * d2.nElements).toInt, d2.window)
  }
}