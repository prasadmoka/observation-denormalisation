package org.syngenta.denormalizer.cache

import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import org.syngenta.denormalizer.config.DenormalizerConfig
import redis.clients.jedis.Jedis

class RedisConnect(redisHost: String, redisPort: Int, jobConfig: DenormalizerConfig) extends java.io.Serializable {

  val config: Config = jobConfig.config
  private val logger = LoggerFactory.getLogger(classOf[RedisConnect])


  private def getConnection(backoffTimeInMillis: Long): Jedis = {
    val defaultTimeOut = jobConfig.redisDbTimeOut
    if (backoffTimeInMillis > 0) try Thread.sleep(backoffTimeInMillis)
    catch {
      case e: InterruptedException =>
        e.printStackTrace()
    }
    logger.info("Obtaining new Redis connection...")
    new Jedis(redisHost, redisPort, defaultTimeOut)
  }


  def getConnection(db: Int, backoffTimeInMillis: Long): Jedis = {
    val jedis: Jedis = getConnection(backoffTimeInMillis)
    jedis.select(db)
    jedis
  }

  def getConnection(db: Int): Jedis = {
    val jedis = getConnection(db, backoffTimeInMillis = 0)
    jedis.select(db)
    jedis
  }

  def getConnection: Jedis = getConnection(db = 0)
}
