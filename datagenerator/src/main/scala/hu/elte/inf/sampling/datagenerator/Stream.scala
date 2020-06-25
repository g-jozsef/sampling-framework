package hu.elte.inf.sampling.datagenerator

trait Stream[T] {
  /**
   * Initializes the stream
   */
  def init(): Unit

  /**
   * Restarts the streamgenerator
   */
  def restart(): Unit

  /**
   * Generates the next item
   *
   * @return Next generated shopId
   */
  def next(): T
}
