package hu.elte.inf.sampling.decisioncore.gedik

class BalancePenalty(val theta: Double,
                     val loadFunc: Double => Double,
                     val numPartitions: Int) {

  private var loads: Array[Double] = Array.fill[Double](numPartitions)(0.0d)
  private var maxLoad: Double = Double.NegativeInfinity
  private var minLoad: Double = Double.PositiveInfinity
  private var avgLoad: Double = 0.0d

  def penalty: Double = {
    (maxLoad - minLoad) / (theta * avgLoad)
  }

  def withAddedWeights(ws: Seq[(Int, Double)]): BalancePenalty =
    withAddedLoads(ws.map {
      case (partition, weight) => (partition, Math.signum(weight) * loadFunc(Math.abs(weight)))
    })

  protected def withAddedLoads(addedLoads: Seq[(Int, Double)]): BalancePenalty = {
    val newBalance = new BalancePenalty(theta, loadFunc, numPartitions)
    newBalance.loads = loads.clone()

    addedLoads.foreach {
      case (partition: Int, load: Double) =>
        newBalance.loads(partition) += load
    }

    newBalance.loads.foreach {
      load: Double => {
        if (load > newBalance.maxLoad) {
          newBalance.maxLoad = load
        }
        if (load < newBalance.minLoad) {
          newBalance.minLoad = load
        }
        newBalance.avgLoad += load
      }
    }
    newBalance.avgLoad /= newBalance.loads.length

    newBalance
  }
}

object BalancePenalty {
  def apply(theta: Double, loadFunc: Double => Double, numPartitions: Int): BalancePenalty =
    new BalancePenalty(theta, loadFunc, numPartitions)
}
