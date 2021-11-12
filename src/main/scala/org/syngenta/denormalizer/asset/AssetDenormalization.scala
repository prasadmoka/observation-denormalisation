package org.syngenta.denormalizer.asset

import org.joda.time.DateTime
import org.syngenta.denormalizer.cache.{DataCache, RedisConnect}
import org.syngenta.denormalizer.config.DenormalizerConfig
import org.syngenta.denormalizer.util.JSONUtil

import java.util

class AssetDenormalization(config: DenormalizerConfig) {

  private val assetDataCache: DataCache =
    new DataCache(config, new RedisConnect(config.redisHost, config.redisPort, config),
      config.redisAssetStore, config.assetFields)

  assetDataCache.init()

  def denormalize(inputMap: util.Map[String, AnyRef]): util.Map[String, AnyRef] = {
    val assetId = inputMap.get("assetRef").asInstanceOf[String]

    if(assetDataCache.isExists(assetId)){
      println("Getting asset details from cache for assetId:"+assetId)
     assetDataCache.getWithRetry(assetId)
    } else {
      //TODO Need to find the fields from which we can find the required details
      println("Could not find the assetDetails in cache for assetId:"+assetId)
      val integrationAccount = inputMap.get("integrationAccountRef")
      val collectionMap = new util.HashMap[String, AnyRef]()
      collectionMap.put("id", assetId)
      collectionMap.put("assetType", "Drone")
      collectionMap.put("created", new DateTime().toString)
      collectionMap.put("integrationAccount", integrationAccount)
      collectionMap.put("manufacturerId", "Syngenta")
      collectionMap.put("brandId", "Phillips")
      assetDataCache.hmSet(assetId,JSONUtil.serialize(collectionMap))
      collectionMap
    }
  }

  def closeDataCache() = {
    assetDataCache.close()
  }
}
