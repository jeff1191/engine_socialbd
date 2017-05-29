package ucm.socialbd.com.process

import java.net.{InetAddress, InetSocketAddress}

import net.liftweb.json.Extraction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import ucm.socialbd.com.config.SocialBDProperties
import ucm.socialbd.com.dataypes.EnrichmentFileModel.{AirFile, AirTrafficFile, TrafficFile}
import ucm.socialbd.com.dataypes.EnrichmentModel.ETraffic
import ucm.socialbd.com.dataypes.EnrichmentObj
import ucm.socialbd.com.dataypes.RawModel.{InterUrbanTraffic, UrbanTraffic}
import ucm.socialbd.com.factory.{DataTypeFactory, Instructions}
import ucm.socialbd.com.sinks.{SimpleElasticsearchSink, WriterSinkStream}
import ucm.socialbd.com.sources.KafkaFactoryConsumer
import ucm.socialbd.com.utils.SocialBDConfig

import scala.io.Source._

/**
  * Created by Jeff on 25/02/2017.
  */
class TrafficStream(socialBDProperties: SocialBDProperties) extends StreamTransform{

  override def process(): Unit = {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val urbanTrafficDataStream: DataStream[UrbanTraffic] = KafkaFactoryConsumer.getRawStream(env,socialBDProperties,Instructions.GET_RAW_URBAN_TRAFFIC).asInstanceOf[DataStream[UrbanTraffic]]
  val interUrbanTrafficDataStream: DataStream[InterUrbanTraffic] = KafkaFactoryConsumer.getRawStream(env,socialBDProperties,Instructions.GET_RAW_INTERURBAN_TRAFFIC).asInstanceOf[DataStream[InterUrbanTraffic]]

  val urbanFileList = fromFile(getClass.getResource("/enrichmentUrbanTraffic.json").getPath).getLines.toList
    .map(x =>  DataTypeFactory.getFileObject(x, Instructions.CREATE_ENRICHMENT_FILE_TRAFFIC).asInstanceOf[TrafficFile])

  val interUrbanFileList = fromFile(getClass.getResource("/enrichmentInterurbanTraffic.json").getPath).getLines.toList
    .map(x =>  DataTypeFactory.getFileObject(x, Instructions.CREATE_ENRICHMENT_FILE_TRAFFIC).asInstanceOf[TrafficFile])

  val enrichmentUrbanDataStream: DataStream[ETraffic] = urbanTrafficDataStream.keyBy(_.ocupacion).map{rawObj =>
    val enrList = urbanFileList.filter(_.cod_cent == rawObj.codigo)
    var enrFile = TrafficFile("Unknown","Unknown","Unknown","Unknown","Unknown","-1","-1")
    if(enrList.size > 0)
      enrFile = enrList.head

    //495 it means urban moreover vmed it's only available for interurban type flag -> -1
    ETraffic(enrFile.gid,rawObj.codigo,enrFile.nombre,rawObj.timestamp,"495", rawObj.intensidad, rawObj.carga,"-1",rawObj.error,enrFile.Xcoord,enrFile.Ycoord)
  }

  val enrichmentInterUrbanDataStream: DataStream[ETraffic] = interUrbanTrafficDataStream.keyBy(_.ocupacion).map{rawObj =>
      val enrList = interUrbanFileList.filter(_.cod_cent == rawObj.codigo)
      var enrFile = TrafficFile("Unknown","Unknown","Unknown","Unknown","Unknown","-1","-1")
      if(enrList.size > 0)
        enrFile = enrList.head

      //494 it means interurban
      ETraffic(enrFile.gid,rawObj.codigo,enrFile.nombre,rawObj.timestamp,"494", rawObj.intensidad, rawObj.carga,rawObj.velocidad,rawObj.error,enrFile.Xcoord,enrFile.Ycoord)
  }
    val jsonString = enrichmentInterUrbanDataStream.union(enrichmentUrbanDataStream)
                  .map(enrObj => DataTypeFactory.getJsonString(enrObj, Instructions.GET_JSON_TRAFFIC).toString)

    writeDataStreamToSinks(jsonString)
  // execute the transformation pipeline
  env.execute("Traffic Job SocialBigData-CM")
  }
  override def writeDataStreamToSinks(enrDataStream: DataStream[String]): Unit = {
    val writer = new WriterSinkStream(socialBDProperties ,enrDataStream)

    socialBDProperties.outputMode.toUpperCase.split(",").foreach(_.trim match {
      case "KAFKA" => writer.writeDataStreamToKafka(socialBDProperties.trafficConf.trafficTopicOut)
      case "FILE" => writer.writeDataStreamToFile(socialBDProperties.trafficConf.outputDir)
      case "ELASTIC" => writer.writeDataStreamToElastic(socialBDProperties.trafficConf.elasticIndex,socialBDProperties.trafficConf.elasticType)
    })
  }
}
