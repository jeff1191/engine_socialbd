package ucm.socialbd.com.config

import com.typesafe.config.ConfigFactory
/**
  * Created by Jeff on 16/04/2017.
  */

case class TwitterConf(twitterTopicIn: String, twitterTopicOut:String,elasticIndex:String,elasticType:String)
case class TrafficConf(trafficTopicIn:String, trafficTopicOut: String,elasticIndex:String,elasticType:String)
case class QualityAirConf(qualityAirTopicIn:String, qualityAirTopicOut:String,elasticIndex:String,elasticType:String)
@SerialVersionUID(100L)
class SocialBDProperties extends Serializable{
  //checks about if exist components in application.conf
  ConfigFactory.load().checkValid(ConfigFactory.defaultReference(), "twitter")
  ConfigFactory.load().checkValid(ConfigFactory.defaultReference(), "traffic")
  ConfigFactory.load().checkValid(ConfigFactory.defaultReference(), "qualityAir")
  ConfigFactory.load().checkValid(ConfigFactory.defaultReference(), "kafkaBrokersUrls")
  ConfigFactory.load().checkValid(ConfigFactory.defaultReference(), "zkUrl")
  ConfigFactory.load().checkValid(ConfigFactory.defaultReference(), "elasticClusterName")
  ConfigFactory.load().checkValid(ConfigFactory.defaultReference(), "elasticNodeName")
  ConfigFactory.load().checkValid(ConfigFactory.defaultReference(), "elasticPort")
  ConfigFactory.load().checkValid(ConfigFactory.defaultReference(), "elasticUrl")

  val twitterConf = TwitterConf(ConfigFactory.load().getString("twitter.twitterTopicIn"),
                    ConfigFactory.load().getString("twitter.twitterTopicOut"),
                    ConfigFactory.load().getString("twitter.elasticIndex"),
                    ConfigFactory.load().getString("twitter.elasticType"))

 val trafficConf = TrafficConf(ConfigFactory.load().getString("traffic.trafficTopicIn"),
                   ConfigFactory.load().getString("traffic.trafficTopicOut"),
                   ConfigFactory.load().getString("traffic.elasticIndex"),
                   ConfigFactory.load().getString("traffic.elasticType"))

  val qualityAirConf = QualityAirConf(ConfigFactory.load().getString("qualityAir.qualityAirTopicIn"),
                  ConfigFactory.load().getString("qualityAir.qualityAirTopicOut"),
                  ConfigFactory.load().getString("qualityAir.elasticIndex"),
                  ConfigFactory.load().getString("qualityAir.elasticType"))

val kafkaBrokersUrls =  ConfigFactory.load().getString("kafkaBrokersUrls")
val zkUrl =  ConfigFactory.load().getString("zkUrl")
val elasticClusterName =  ConfigFactory.load().getString("elasticClusterName")
val elasticNodeName =  ConfigFactory.load().getString("elasticNodeName")
val elasticPort =  ConfigFactory.load().getInt("elasticPort")
val elasticUrl =  ConfigFactory.load().getString("elasticUrl")

}
