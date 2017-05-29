package ucm.socialbd.com.process

import java.net.{InetAddress, InetSocketAddress}

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import twitter4j.TwitterObjectFactory
import ucm.socialbd.com.config.SocialBDProperties
import ucm.socialbd.com.dataypes.EnrichmentObj
import ucm.socialbd.com.dataypes.RawModel.Twitter
import ucm.socialbd.com.factory.{DataTypeFactory, Instructions}
import ucm.socialbd.com.sinks.{SimpleElasticsearchSink, WriterSinkStream}
import ucm.socialbd.com.sources.KafkaFactoryConsumer
import ucm.socialbd.com.utils.SocialBDConfig

/**
  * Created by Jeff on 25/02/2017.
  */
class TwitterStream(socialBDProperties: SocialBDProperties) extends StreamTransform{

  override def process(): Unit = {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = SocialBDConfig.getKafkaProperties(socialBDProperties)
  // Consumer group ID
  //properties.setProperty("auto.offset.reset", "earliest");
  val twitterDataStream:DataStream[Twitter] = KafkaFactoryConsumer.getRawStream(env,socialBDProperties,Instructions.GET_RAW_TWITTER).asInstanceOf[DataStream[Twitter]]
    val jsonDataStream  = twitterDataStream
      .map(enrObj => DataTypeFactory.getJsonString(enrObj, Instructions.GET_JSON_TWITTER).toString)
    writeDataStreamToSinks(jsonDataStream)
    // execute the transformation pipeline
    env.execute("Twitter Job SocialBigData-CM")
}
  override def writeDataStreamToSinks(enrDataStream: DataStream[String]): Unit = {
    val writer = new WriterSinkStream(socialBDProperties ,enrDataStream)

    socialBDProperties.outputMode.toUpperCase.split(",").foreach(_.trim match {
      case "KAFKA" => writer.writeDataStreamToKafka(socialBDProperties.twitterConf.twitterTopicOut)
      case "FILE" => writer.writeDataStreamToFile(socialBDProperties.twitterConf.outputDir)
      case "ELASTIC" => writer.writeDataStreamToElastic(socialBDProperties.twitterConf.elasticIndex,socialBDProperties.twitterConf.elasticType)
    })
  }

}
