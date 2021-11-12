package org.syngenta.denormalizer.util

import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.KafkaSource

import java.util
import org.syngenta.denormalizer.config.DenormalizerConfig
import org.syngenta.denormalizer.serde.{ByteDeserializationSchema, ByteSerializationSchema, MapDeserializationSchema,
  MapSerializationSchema, StringDeserializationSchema, StringSerializationSchema}

class FlinkKafkaConnector(config: DenormalizerConfig)  {
  def kafkaMapSource(kafkaTopic: String): KafkaSource[util.Map[String, AnyRef]] = {
    KafkaSource.builder[util.Map[String, AnyRef]]()
      .setTopics(kafkaTopic)
      .setDeserializer(new MapDeserializationSchema)
      .setProperties(config.kafkaConsumerProperties)
      .build()
  }

  def kafkaMapSink(kafkaTopic: String): KafkaSink[util.Map[String, AnyRef]] = {
    KafkaSink.builder[util.Map[String, AnyRef]]()
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .setRecordSerializer(new MapSerializationSchema(kafkaTopic))
      .setKafkaProducerConfig(config.kafkaProducerProperties)
      .build()
  }

  def kafkaStringSource(kafkaTopic: String): KafkaSource[String] = {
    KafkaSource.builder[String]()
      .setTopics(kafkaTopic)
      .setDeserializer(new StringDeserializationSchema)
      .setProperties(config.kafkaConsumerProperties)
      .setBootstrapServers(config.kafkaConsumerBrokerServers)
      .build()
  }

  def kafkaBytesSource(kafkaTopic: String): KafkaSource[Array[Byte]] = {
    KafkaSource.builder[Array[Byte]]()
      .setTopics(kafkaTopic)
      .setDeserializer(new ByteDeserializationSchema)
      .setProperties(config.kafkaConsumerProperties)
      .build()
  }

  def kafkaStringSink(kafkaTopic: String): KafkaSink[String] = {
    KafkaSink.builder[String]()
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .setRecordSerializer(new StringSerializationSchema(kafkaTopic))
      .setKafkaProducerConfig(config.kafkaProducerProperties)
      .setBootstrapServers(config.kafkaProducerBrokerServers)
      .build()
  }


  def kafkaBytesSink(kafkaTopic: String): KafkaSink[Array[Byte]] = {
    KafkaSink.builder[Array[Byte]]()
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .setRecordSerializer(new ByteSerializationSchema(kafkaTopic))
      .setKafkaProducerConfig(config.kafkaProducerProperties)
      .build()
  }
}
