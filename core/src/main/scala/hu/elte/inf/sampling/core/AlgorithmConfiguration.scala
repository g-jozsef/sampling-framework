package hu.elte.inf.sampling.core

import hu.elte.inf.sampling.core.HyperParameter.OptimizedHyperParameter

import scala.reflect.ClassTag

class AlgorithmConfiguration(algorithmName: String,
                             parameters: Map[String, HyperParameter[Any]] = Map.empty[String, HyperParameter[Any]]) {

  def this(algorithmName: String, parameters: Iterable[(String, HyperParameter[Any])]) = {
    this(algorithmName, parameters.toMap)
  }

  def this(algorithmName: String, parameters: (String, HyperParameter[Any])*) = {
    this(algorithmName, parameters.toMap)
  }

  val uID: Int = AlgorithmConfiguration.getNewUID

  def longName: String = "[" + uID.toString + "]_" + toString

  def shortName: String = {
    s"[${uID.toString}] ${algorithmName}"
  }

  def getParam[T: ClassTag](parameterSymbol: String): T = {
    if (parameters.isEmpty) {
      throw new IllegalStateException("No parameters found.")
    }

    parameters.get(parameterSymbol) match {
      case Some(existingParam) =>
        existingParam.value match {
          case value: T =>
            value
          case value =>
            throw new IllegalArgumentException(s"Parameter type mismatch; parameter: ${parameterSymbol}, value: ${value.toString}")
        }
      case None =>
        throw new IllegalArgumentException("Unknown parameter: " + parameterSymbol)
    }
  }

  def getOptimizedParameters: Iterable[(String, OptimizedHyperParameter[Any])] =
    parameters.collect {
      case (parameterName, optimizedHyperParameter: OptimizedHyperParameter[Any]) => (parameterName, optimizedHyperParameter)
    }

  def getAllParams: Map[String, HyperParameter[Any]] =
    parameters

  override def toString: String = {
    s"[${uID.toString}] ${algorithmName}(" +
      parameters.map {
        case (name: String, param: HyperParameter[Any]) =>
          s"$name=${param.value.toString}"
      }.mkString(",") +
      s")"
  }

  def copy(paramOverrides: Map[String, HyperParameter[Any]]): AlgorithmConfiguration = {
    new AlgorithmConfiguration(algorithmName, parameters ++ paramOverrides)
  }
}

object AlgorithmConfiguration {
  var gIDCounter = 0

  def getNewUID: Int = synchronized {
    gIDCounter = gIDCounter + 1
    gIDCounter
  }
}