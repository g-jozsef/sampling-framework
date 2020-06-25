package hu.elte.inf.sampling.core

import hu.elte.inf.sampling.core.StreamConfig.{DistrConfig, DriftConfig}

case class StreamConfig(describe: String,
                        nElements: Int,
                        nShops: Int,
                        fileName: String,
                        initialDistribution: DistrConfig,
                        drifters: DriftConfig*) {
}

object StreamConfig {

  case class DistrConfig(distr: String, seed: Int, params: Double*)

  case class DriftConfig(distrConfig: DistrConfig, offsetPrc: Double, driftWindow: Int)

}