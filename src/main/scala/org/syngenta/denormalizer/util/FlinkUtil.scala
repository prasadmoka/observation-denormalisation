package org.syngenta.denormalizer.util

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.syngenta.denormalizer.config.DenormalizerConfig

object FlinkUtil {

  def getExecutionContext(config: DenormalizerConfig): StreamExecutionEnvironment = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env
  }
}
