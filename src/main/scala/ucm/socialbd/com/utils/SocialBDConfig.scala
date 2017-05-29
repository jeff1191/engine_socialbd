package ucm.socialbd.com.utils

import java.net.{InetAddress, InetSocketAddress}
import java.util
import java.util.Properties

import ucm.socialbd.com.config.SocialBDProperties

/**
  * Created by Jeff on 16/04/2017.
  */
object SocialBDConfig {

  def getKafkaProperties(socialBDProperties: SocialBDProperties): Properties  = {
    val properties = new Properties()
    // comma separated list of Kafka brokers
    properties.setProperty("bootstrap.servers", socialBDProperties.kafkaBrokersUrls)
    properties.setProperty("zookeeper.connect", socialBDProperties.zkUrl)
    // id of the consumer group
    properties.setProperty("group.id", "test")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("key-class-type", "java.lang.String")
    properties.put("value-class-type", "java.lang.String")
    properties
  }

  def getElasticConfiguration(socialBDProperties: SocialBDProperties): util.HashMap[String, String] = {
    val config = new java.util.HashMap[String, String]
    config.put("bulk.flush.max.actions", "1")
    config.put("cluster.name", socialBDProperties.elasticClusterName)
    config.put("node.name", socialBDProperties.elasticNodeName)
    config
  }

}
