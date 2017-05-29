package ucm.socialbd.com.sources

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import ucm.socialbd.com.config.SocialBDProperties
import ucm.socialbd.com.dataypes.RawObj
import ucm.socialbd.com.factory.{DataTypeFactory, Instructions}
import ucm.socialbd.com.utils.SocialBDConfig
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import twitter4j.TwitterObjectFactory
import ucm.socialbd.com.dataypes.RawModel.{Twitter, TwitterUser}
/**
  * Created by Jeff on 15/04/2017.
  */
object KafkaFactoryConsumer {


  def getRawStream(env:StreamExecutionEnvironment, socialBDProperties: SocialBDProperties,ins: String): DataStream[RawObj] = {
    //common properties
    val properties = SocialBDConfig.getKafkaProperties(socialBDProperties)
    ins match {
      case Instructions.GET_RAW_AIR =>
        //properties.setProperty("auto.offset.reset", "earliest")
        val streamAirKafka: DataStream[String]= env.addSource(
          new FlinkKafkaConsumer010[String](socialBDProperties.qualityAirConf.qualityAirTopicIn, new SimpleStringSchema(), properties))

        streamAirKafka.map(x => DataTypeFactory.getRawObject(x,Instructions.CREATE_RAW_AIR))

      case Instructions.GET_RAW_URBAN_TRAFFIC =>
        val streamUrbanTrafficKafka: DataStream[String]= env.addSource(new FlinkKafkaConsumer010[String](
          socialBDProperties.trafficConf.urbanTrafficTopicIn, new SimpleStringSchema(), properties))

        streamUrbanTrafficKafka.map(x => DataTypeFactory.getRawObject(x,Instructions.CREATE_RAW_URBAN_TRAFFIC))

      case Instructions.GET_RAW_INTERURBAN_TRAFFIC =>
        val streamInterUrbanTrafficKafka: DataStream[String]= env.addSource(new FlinkKafkaConsumer010[String](
          socialBDProperties.trafficConf.interUrbanTrafficTopicIn, new SimpleStringSchema(), properties))

        streamInterUrbanTrafficKafka.map(x => DataTypeFactory.getRawObject(x,Instructions.CREATE_RAW_INTERURBAN_TRAFFIC))

      case Instructions.GET_RAW_TWITTER =>
        val streamTwitterKafka: DataStream[String]= env.addSource(new FlinkKafkaConsumer010[String](
        socialBDProperties.twitterConf.twitterTopicIn, new SimpleStringSchema(), properties))

        streamTwitterKafka.map(tweetjson => DataTypeFactory.getRawObject(tweetjson,Instructions.CREATE_RAW_TWITTER))
      case Instructions.GET_RAW_BICIMAD =>
        val streamBiciKafka: DataStream[String]= env.addSource(new FlinkKafkaConsumer010[String](
        socialBDProperties.biciMADConf.bicimadTopicIn, new SimpleStringSchema(), properties))

        streamBiciKafka.map(x => DataTypeFactory.getRawObject(x,Instructions.CREATE_RAW_BICIMAD))
      case Instructions.GET_RAW_EMTBUS =>
        val streamTEmtBusKafka: DataStream[String]= env.addSource(new FlinkKafkaConsumer010[String](
          socialBDProperties.eMTBusConf.emtbusTopicIn, new SimpleStringSchema(), properties))

        streamTEmtBusKafka.map(x => DataTypeFactory.getRawObject(x,Instructions.CREATE_RAW_EMTBUS))

      case _ =>  throw new Exception(s"Unknown kafka source ${ins}")
    }
  }

}
