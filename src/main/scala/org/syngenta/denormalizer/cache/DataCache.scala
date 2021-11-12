package org.syngenta.denormalizer.cache

import com.google.gson.Gson
import org.slf4j.LoggerFactory
import org.syngenta.denormalizer.config.DenormalizerConfig
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisException

import java.util
import scala.collection.JavaConverters._

class DataCache(val config: DenormalizerConfig, val redisConnect: RedisConnect, val dbIndex: Int, val fields: List[String]) {

  private[this] val logger = LoggerFactory.getLogger(classOf[DataCache])
  private var redisConnection: Jedis = _
  val gson = new Gson()

  def init() {
    this.redisConnection = redisConnect.getConnection(dbIndex)
  }

  def close() {
    this.redisConnection.close()
  }

  def getWithRetry(key: String): util.Map[String, AnyRef] = {
    try {
      get(key)
    } catch {
      case ex: JedisException =>
        logger.error("Exception when retrieving data from redis cache", ex)
        this.redisConnection.close()
        this.redisConnection = redisConnect.getConnection(dbIndex)
        get(key)
    }

  }

  private def get(key: String): util.Map[String, AnyRef] = {
    val data = redisConnection.get(key)
    println("Data is:"+data)
    if (data != null && !data.isEmpty()) {
      val dataMap = gson.fromJson(data, new util.HashMap[String, AnyRef]().getClass)
      if(fields.nonEmpty) dataMap.keySet().retainAll(fields.asJava)
      dataMap.values().removeAll(util.Collections.singleton(""))
      dataMap
    } else {
      new util.HashMap[String, AnyRef]()
    }
  }

  def isExists(key: String): Boolean = {
    redisConnection.exists(key)
  }

  def hmSet(key: String, value: String): Unit = {
    try {
      redisConnection.setnx(key, value)
    } catch {
      case ex: JedisException => {
        println("dataCache")
        logger.error("Exception when inserting data to redis cache", ex)
        this.redisConnection.close()
        this.redisConnection = redisConnect.getConnection(dbIndex)
        this.redisConnection.setnx(key, value)
      }
    }
  }


}
