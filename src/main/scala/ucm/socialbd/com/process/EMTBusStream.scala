package ucm.socialbd.com.process

import java.net.{InetAddress, InetSocketAddress}

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import ucm.socialbd.com.config.SocialBDProperties
import ucm.socialbd.com.dataypes.EnrichmentObj
import ucm.socialbd.com.dataypes.RawModel.EMTBus
import ucm.socialbd.com.factory.{DataTypeFactory, Instructions}
import ucm.socialbd.com.sinks.{SimpleElasticsearchSink, WriterSinkStream}
import ucm.socialbd.com.sources.KafkaFactoryConsumer
import ucm.socialbd.com.utils.SocialBDConfig

/**
  * Created by Jeff on 14/05/2017.
  */
class EMTBusStream(socialBDProperties: SocialBDProperties) extends StreamTransform{
  override def process(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val emtBusStream: DataStream[EMTBus] = KafkaFactoryConsumer.getRawStream(env,socialBDProperties,Instructions.GET_RAW_EMTBUS).asInstanceOf[DataStream[EMTBus]]
    val jsonDataStream  = emtBusStream
      .map(enrObj => DataTypeFactory.getJsonString(enrObj, Instructions.GET_JSON_EMTBUS).toString)

    writeDataStreamToSinks(jsonDataStream)
    env.execute("EMTBus Job SocialBigData-CM")
  }

  override def writeDataStreamToSinks(enrDataStream: DataStream[String]): Unit = {
    val writer = new WriterSinkStream(socialBDProperties ,enrDataStream)

    socialBDProperties.outputMode.toUpperCase.split(",").foreach(_.trim match {
      case "KAFKA" => writer.writeDataStreamToKafka(socialBDProperties.eMTBusConf.emtbusTopicOut)
      case "FILE" => writer.writeDataStreamToFile(socialBDProperties.eMTBusConf.outputDir)
      case "ELASTIC" => writer.writeDataStreamToElastic(socialBDProperties.eMTBusConf.elasticIndex,socialBDProperties.eMTBusConf.elasticType)
    })
  }
}
