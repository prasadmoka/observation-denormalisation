package org.syngenta.denormalizer.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.syngenta.denormalizer.config.DenormalizerConfig
import org.syngenta.denormalizer.functions.DenormalizedKeyFunction
import org.syngenta.denormalizer.util.{FlinkKafkaConnector, FlinkUtil}

import java.util

class DenormalizerKeyStreamTask(config: DenormalizerConfig, kafkaConnector: FlinkKafkaConnector) {


  implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)

  def process(): Unit = {
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    //val ipStream: DataStreamSource[String] = env.readTextFile("src/main/resources/obs-input")
    //ipStream.sinkTo(kafkaConnector.kafkaStringSink(config.kafkaInputTopic))

    val kafkaConsumer: KafkaSource[util.Map[String, AnyRef]] = kafkaConnector.kafkaMapSource(config.kafkaInputTopic)

    //deNormalisationConsumer
    val denormStream = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks[util.Map[String, AnyRef]](),
      config.deNormalisationConsumer)
      .uid(config.deNormalisationConsumer)
      .setParallelism(config.deNormalisationParallelism).rebalance()
      .keyBy((value: util.Map[String, AnyRef]) => {
        value.get("assetRef").asInstanceOf[String]
      }).process(new DenormalizedKeyFunction(config)).name(config.deNormalisationFunction)
      .uid(config.deNormalisationFunction)
      .setParallelism(config.deNormalisationParallelism)

    denormStream.getSideOutput(config.denormAssetsTag)

    env.execute()
  }

}
  object DenormalizerKeyStreamTask{
    def main(args: Array[String]): Unit = {
      val config = ConfigFactory.load("de-normaliser.conf")
      val denormConfig = new DenormalizerConfig(config,"Denormalizer")
      val flinkConnetor = new FlinkKafkaConnector(denormConfig)
      val streamTask = new DenormalizerKeyStreamTask(denormConfig,flinkConnetor)
      streamTask.process()
    }

}
