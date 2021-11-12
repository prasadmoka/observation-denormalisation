package org.syngenta.denormalizer.task

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.syngenta.denormalizer.config.DenormalizerConfig
import org.syngenta.denormalizer.functions.DenormalizerFunction
import org.syngenta.denormalizer.util.{FlinkKafkaConnector, FlinkUtil}
import java.util

class DenormalizerStreamTask(config: DenormalizerConfig, kafkaConnector: FlinkKafkaConnector) {
  implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)

  def process(): Unit = {
    implicit val stringTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    val kafkaConsumer: KafkaSource[util.Map[String, AnyRef]] = kafkaConnector.kafkaMapSource(config.kafkaInputTopic)

    //deNormalisationConsumer
    val denormStream = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks[util.Map[String, AnyRef]](), config.deNormalisationConsumer)
      .uid(config.deNormalisationConsumer)
        .setParallelism(config.deNormalisationParallelism).rebalance()
        .process(new DenormalizerFunction(config)).name(config.deNormalisationFunction)
          .uid(config.deNormalisationFunction)
        .setParallelism(config.deNormalisationParallelism)

    denormStream.getSideOutput(config.denormAssetsTag)

    env.execute()
  }
}


