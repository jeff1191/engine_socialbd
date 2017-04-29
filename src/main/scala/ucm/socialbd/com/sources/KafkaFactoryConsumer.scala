package ucm.socialbd.com.sources

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import ucm.socialbd.com.config.SocialBDProperties
import ucm.socialbd.com.dataypes.RawObj
import ucm.socialbd.com.factory.{DataTypeFactory, Instructions}
import ucm.socialbd.com.utils.SocialBDConfig
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
/**
  * Created by Jeff on 15/04/2017.
  */
object KafkaFactoryConsumer {


  def getRawStream(env:StreamExecutionEnvironment, socialBDProperties: SocialBDProperties,ins: String): DataStream[RawObj] = {
    //common properties
    val properties = SocialBDConfig.getProperties(socialBDProperties)
    ins match {
      case Instructions.GET_RAW_AIR =>
        //properties.setProperty("auto.offset.reset", "earliest")
        val streamAirKafka: DataStream[String]= env.addSource(
          new FlinkKafkaConsumer010[String](socialBDProperties.qualityAirConf.qualityAirTopicIn, new SimpleStringSchema(), properties))

        streamAirKafka.map(x => DataTypeFactory.getRawObject(x,Instructions.CREATE_RAW_AIR))

      case Instructions.GET_RAW_TRAFFIC =>
        val streamTrafficKafka: DataStream[String]= env.addSource(new FlinkKafkaConsumer010[String](
          socialBDProperties.trafficConf.trafficTopicIn, new SimpleStringSchema(), properties))

        streamTrafficKafka.map(x => DataTypeFactory.getRawObject(x,Instructions.CREATE_RAW_TRAFFIC))

      case Instructions.GET_RAW_TWITTER => null

      case _ =>  throw new Exception(s"Unknown kafka source ${ins}")
    }
  }

}
