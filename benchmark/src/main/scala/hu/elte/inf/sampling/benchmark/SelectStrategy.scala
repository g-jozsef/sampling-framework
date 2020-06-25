package hu.elte.inf.sampling.benchmark

import scala.util.Random

trait SelectStrategy[Config, Result] {
  def select(confs: Seq[(Config, Result)]): Seq[(Config, Result)]
}

object SelectStrategy {

  class GeneticSelector[Config, Result](prcSurvivors: Double, hardLimit: Int) extends SelectStrategy[Config, Result] {
    val r: Random = Random

    override def select(confs: Seq[(Config, Result)]): Seq[(Config, Result)] = {
      val survivors = confs.take((confs.length * prcSurvivors).toInt).take(hardLimit)

      survivors
    }
  }

  class RandomLocalMinimumSearchSelector[Config, Result]() extends SelectStrategy[Config, Result] {

    override def select(confs: Seq[(Config, Result)]): Seq[(Config, Result)] = {
      Seq(confs.head)
    }
  }

}
