package org.syngenta.denormalizer.spec

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses
import org.scalatest.{FlatSpec, Matchers}
import org.syngenta.denormalizer.config.DenormalizerConfig
import org.syngenta.denormalizer.fixtures.ObservationData
import org.syngenta.denormalizer.functions.DenormalizerFunction
import java.util._

class DenormalizerFunctionTest extends FlatSpec with Matchers{
  val config: Config = ConfigFactory.load("de-normaliser.conf")
  val transformerConfig = new DenormalizerConfig(config, "obs-schema-transformer")
  //instantiate created user defined function
  val processFunction = new DenormalizerFunction(transformerConfig)
  val objMapper = new ObjectMapper

  "DenormalizerFunction" should "write data into cache" in {
    // wrap user defined function into a the corresponding operator
    val harness = ProcessFunctionTestHarnesses.forProcessFunction(processFunction)
    //StreamRecord object with the sample data
    val dataTest: Map[String,AnyRef] = objMapper.readValue(ObservationData.OBS_VALID_EVENT,classOf[Map[String,AnyRef]])
    val testData: StreamRecord[Map[String,AnyRef]] = new StreamRecord[Map[String,AnyRef]](dataTest)
    harness.processElement(testData)
    harness.getSideOutput(transformerConfig.denormAssetsTag) should have size 1
    harness.close()
  }

  "DenormalizerFunction" should "write new data into cache" in {
    // wrap user defined function into a the corresponding operator
    val harness = ProcessFunctionTestHarnesses.forProcessFunction(processFunction)
    //StreamRecord object with the sample data
    val dataTest: Map[String,AnyRef] = objMapper.readValue(ObservationData.OBS_VALID_EVENT,classOf[Map[String,AnyRef]])
    val testData: StreamRecord[Map[String,AnyRef]] = new StreamRecord[Map[String,AnyRef]](dataTest)
    harness.processElement(testData)
    harness.getSideOutput(transformerConfig.denormAssetsTag) should have size 1
    harness.close()
  }


}
