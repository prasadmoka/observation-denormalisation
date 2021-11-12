package org.syngenta.denormalizer.config

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

import java.util.Properties

class DenormalizerConfig(val config: Config, val jobName: String) extends Serializable {

  private val serialVersionUID = -4515020556926788923L
  implicit val metaTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val kafkaProducerBrokerServers: String = config.getString("kafka.producer.broker-servers")
  val kafkaConsumerBrokerServers: String = config.getString("kafka.consumer.broker-servers")
  // Producer Properties
  val kafkaProducerMaxRequestSize: Int = config.getInt("kafka.producer.max-request-size")
  val kafkaProducerBatchSize: Int = config.getInt("kafka.producer.batch.size")
  val kafkaProducerLingerMs: Int = config.getInt("kafka.producer.linger.ms")
  val groupId: String = config.getString("kafka.groupId")
  val kafkaInputTopic: String = config.getString("kafka.input.topic")

  val deNormalisationParallelism: Int = config.getInt("task.denormalisation.parallelism")


  // Only for Tests
  val kafkaAutoOffsetReset: Option[String] = if (config.hasPath("kafka.auto.offset.reset")) Option(config.getString("kafka.auto.offset.reset")) else None



  def kafkaConsumerProperties: Properties = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", kafkaConsumerBrokerServers)
    properties.setProperty("group.id", groupId)
    properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
    kafkaAutoOffsetReset.map { properties.setProperty("auto.offset.reset", _) }
    properties
  }

  def kafkaProducerProperties: Properties = {
    val properties = new Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProducerBrokerServers)
    properties.put(ProducerConfig.LINGER_MS_CONFIG, Integer.valueOf(kafkaProducerLingerMs))
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.valueOf(kafkaProducerBatchSize))
    properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, Integer.valueOf(kafkaProducerMaxRequestSize))
    properties
  }

  val assetFields = List("id", "assetType", "created", "integrationAccount", "manufacturerId", "brandId")
  val deNormalisationConsumer = "denormalisation-consumer"
  val deNormalisationFunction: String = "DeNormaliserFunction"
  val redisHost = config.getString("redis.host")
  val redisPort = config.getInt("redis.port")
  val redisDbTimeOut = config.getInt("redisdb.connection.timeout")

  val redisAssetStore = config.getInt("redis.database.assetstore.id")

  val denormAssetsTag: OutputTag[String] = OutputTag[String]("denorm-aasets")
}
