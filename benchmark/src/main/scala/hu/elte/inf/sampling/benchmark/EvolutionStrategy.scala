package hu.elte.inf.sampling.benchmark

import hu.elte.inf.sampling.core.HyperParameter.OptimizedNumericHyperParameter
import hu.elte.inf.sampling.core.{AlgorithmConfiguration, HyperParameter, Utils}

import scala.collection.mutable
import scala.util.Random

trait EvolutionStrategy[Config] {
  def evolve(confs: Seq[Config]): Seq[Config]
}

object EvolutionStrategy {

  class GeneticAlgorithm(crossOverIter: Int = 4, mutationChance: Double = 0.2) extends EvolutionStrategy[AlgorithmConfiguration] {
    val r: Random = Random

    override def evolve(confs: Seq[AlgorithmConfiguration]): Seq[AlgorithmConfiguration] = {
      mutation(crossOver(confs.toArray))
    }

    def crossOver(confs: Array[AlgorithmConfiguration]): Seq[AlgorithmConfiguration] = {
      val resultBuilder: mutable.ArrayBuilder[AlgorithmConfiguration] = mutable.ArrayBuilder.make
      var i = 0
      while (i < crossOverIter) {
        val j = r.nextInt(confs.length)
        val k = r.nextInt(confs.length)

        val crossOverParams: mutable.ArrayBuilder[(String, HyperParameter[Any])] = mutable.ArrayBuilder.make

        val iter1 = confs(j).getOptimizedParameters.iterator
        val iter2 = confs(k).getOptimizedParameters.iterator
        assert(confs(j).getOptimizedParameters.size == confs(j).getOptimizedParameters.size)
        while (iter1.nonEmpty) {
          val a = iter1.next()
          val b = iter2.next()
          if (r.nextDouble() > 0.5) {
            crossOverParams += a
          } else {
            crossOverParams += b
          }
        }

        val crossed = confs(j).copy(crossOverParams.result().toMap)
        resultBuilder += crossed

        i += 1
      }

      resultBuilder.result().toSeq
    }

    def mutation(confs: Seq[AlgorithmConfiguration]): Seq[AlgorithmConfiguration] = {
      confs.map(
        x =>
          if (r.nextDouble() < mutationChance) {
            val mutateThese = x.getOptimizedParameters.collect {
              case (s, param: OptimizedNumericHyperParameter[Any]) if r.nextDouble() < mutationChance =>
                (s, param)
            }
            val mutated: Map[String, OptimizedNumericHyperParameter[Any]] = mutateThese.map(
              m => (m._1, m._2.mutate)
            ).toMap

            x.copy(mutated)
          }
          else
            x
      )

    }
  }

  class RandomLocalMinimumSearch(upperlimit: Int) extends EvolutionStrategy[AlgorithmConfiguration] {
    val r: Random = Random

    override def evolve(confs: Seq[AlgorithmConfiguration]): Seq[AlgorithmConfiguration] = {
      assert(confs.size == 1, "Random minimum local search should only get one configuration")

      val baseConf: AlgorithmConfiguration = confs.head

      val allNeighbours: List[List[(String, HyperParameter[Any])]] =
        baseConf.getOptimizedParameters.map(o => o._2.getNeighbours.map(o._1 -> _).toList).toList

      val allCombinations: List[List[(String, HyperParameter[Any])]] =
        Utils.combinations(allNeighbours)

      val newConfs: Iterable[AlgorithmConfiguration] = allCombinations.map(x => baseConf.copy(x.toMap))


      if (newConfs.size > upperlimit) {
        Random.shuffle(newConfs).take(upperlimit).toSeq
      } else {
        newConfs.toSeq
      }
    }

  }

}