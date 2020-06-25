package hu.elte.inf.sampling.core

import java.util.concurrent.ThreadLocalRandom

class RandomNumberGenerator {

  def getNextDouble: Double = {
    ThreadLocalRandom.current().nextDouble()
  }

}
