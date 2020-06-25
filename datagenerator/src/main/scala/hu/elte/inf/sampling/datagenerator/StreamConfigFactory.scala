package hu.elte.inf.sampling.datagenerator

import java.io.Reader

import hu.elte.inf.sampling.core.StreamConfig.{DistrConfig, DriftConfig}
import hu.elte.inf.sampling.core.{Shop, StreamConfig}
import org.json4s.native.Serialization
import org.json4s.{DefaultFormats, Formats, NoTypeHints}

object StreamConfigFactory {
  implicit val formats: Formats = DefaultFormats + NoTypeHints

  def writePretty(streamConfig: StreamConfig): String = {
    Serialization.writePretty(streamConfig)
  }

  def apply(reader: Reader): StreamConfig = Serialization.read[StreamConfig](reader)

  def apply(describe: String,
            nElements: Int,
            nShops: Int,
            fileName: String,
            initialGenerator: DistributionShopStreamGenerator,
            drifters: Drifter[Shop.Type]*): StreamConfig = {

    val initialConf = DistrConfig(initialGenerator.dist.name, initialGenerator.seed, initialGenerator.dist.params: _*)
    val drifterConfs = drifters.map {
      drifter =>
        val shopDistStream = drifter.stream.asInstanceOf[DistributionShopStreamGenerator] // surely, right?
        DriftConfig(DistrConfig(shopDistStream.dist.name, shopDistStream.seed, shopDistStream.dist.params: _*), drifter.offsetPrc, drifter.window)
    }

    new StreamConfig(describe, nElements, nShops, fileName, initialConf, drifterConfs: _*)
  }
}