package hu.elte.inf.sampling.datagenerator

import hu.elte.inf.sampling.core.Logger

import scala.util.Random

/**
 * @param seed seed to use for internal random
 */
abstract class StreamGenerator[TItem](val seed: Int = 0)
  extends Stream[TItem]
    with Serializable
    with Logger {

  protected var rand: Random = _

  override def restart(): Unit = {
    if (seed != 0)
      rand = new Random(seed)
    else
      rand = new Random()
  }

  override def init(): Unit = {
    restart()
  }
}