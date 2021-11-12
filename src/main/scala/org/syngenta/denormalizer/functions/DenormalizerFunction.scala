package org.syngenta.denormalizer.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.syngenta.denormalizer.asset.AssetDenormalization
import org.syngenta.denormalizer.config.DenormalizerConfig

import java.util

class DenormalizerFunction(config: DenormalizerConfig)
  extends ProcessFunction[util.Map[String, AnyRef], String] {

  private val assetDenormalisation: AssetDenormalization = new AssetDenormalization(config)

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def processElement(event: util.Map[String, AnyRef], context: ProcessFunction[util.Map[String, AnyRef],
    String]#Context, out: Collector[String]): Unit = {
    val denormalisedEvent = assetDenormalisation.denormalize(event)

  }

}