package org.syngenta.denormalizer.functions

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.syngenta.denormalizer.asset.AssetDenormalization
import org.syngenta.denormalizer.config.DenormalizerConfig
import org.syngenta.denormalizer.util.JSONUtil

import java.util

class DenormalizedKeyFunction(config: DenormalizerConfig) extends KeyedProcessFunction[String, util.Map[String, AnyRef],
  String]{
  private val assetDenormalisation: AssetDenormalization = new AssetDenormalization(config)

  override def processElement(value: util.Map[String, AnyRef], ctx: KeyedProcessFunction[String, util.Map[String, AnyRef],
    String]#Context, out: Collector[String]): Unit = {
    val denormalisedEvent = assetDenormalisation.denormalize(value)
    ctx.output(config.denormAssetsTag,JSONUtil.serialize(denormalisedEvent))
  }
}
