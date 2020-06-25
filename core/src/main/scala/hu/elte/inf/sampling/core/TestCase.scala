package hu.elte.inf.sampling.core

trait TestCase[+A, +C] {
  def makeAlgorithm: A

  def getConfig: C
}
