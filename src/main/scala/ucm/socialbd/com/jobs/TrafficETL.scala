package ucm.socialbd.com.jobs

import java.net.{InetAddress, InetSocketAddress}

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import ucm.socialbd.com.config.SocialBDProperties
import ucm.socialbd.com.dataypes.EnrichmentFileModel.{AirFile, TrafficFile}
import ucm.socialbd.com.dataypes.EnrichmentObj
import ucm.socialbd.com.dataypes.RawModel.{InterUrbanTraffic, UrbanTraffic}
import ucm.socialbd.com.factory.{DataTypeFactory, Instructions}
import ucm.socialbd.com.sinks.SimpleElasticsearchSink
import ucm.socialbd.com.sources.KafkaFactoryConsumer
import ucm.socialbd.com.utils.SocialBDConfig

import scala.io.Source._

/**
  * Created by Jeff on 25/02/2017.
  */
class TrafficETL(socialBDProperties: SocialBDProperties) extends ETL{

  override def process(): Unit = {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val urbanTrafficDataStream: DataStream[UrbanTraffic] = KafkaFactoryConsumer.getRawStream(env,socialBDProperties,Instructions.GET_RAW_URBAN_TRAFFIC).asInstanceOf[DataStream[UrbanTraffic]]
  val interUrbanTrafficDataStream: DataStream[InterUrbanTraffic] = KafkaFactoryConsumer.getRawStream(env,socialBDProperties,Instructions.GET_RAW_INTERURBAN_TRAFFIC).asInstanceOf[DataStream[InterUrbanTraffic]]


    val urbanFileList = fromFile(getClass.getResource("/enrichmentUrbanTraffic.json").getPath).getLines.toList
      .map(x =>  DataTypeFactory.getFileObject(x, Instructions.CREATE_ENRICHMENT_FILE_TRAFFIC).asInstanceOf[TrafficFile])

    val interUrbanFileList = fromFile(getClass.getResource("/enrichmentInterurbanTraffic.json").getPath).getLines.toList
      .map(x =>  DataTypeFactory.getFileObject(x, Instructions.CREATE_ENRICHMENT_FILE_TRAFFIC).asInstanceOf[TrafficFile])

    urbanFileList.map(x => println(x))
    interUrbanFileList.map(x => println(x))
    //  val unido = urbanTrafficDataStream.connect(interUrbanTrafficDataStream)

  val writeDataStream = urbanTrafficDataStream.writeAsText("raw/urbanTraffic")
  interUrbanTrafficDataStream.writeAsText("raw/interurbanTraffic")
  // execute the transformation pipeline
  env.execute("Traffic Job SocialBigData-CM")
  }
  override def sendToElastic(enrDataStream: DataStream[EnrichmentObj]): Unit = {
    val transports = new java.util.ArrayList[InetSocketAddress]
    transports.add(new InetSocketAddress(InetAddress.getByName(socialBDProperties.elasticUrl), socialBDProperties.elasticPort))

    val config = new java.util.HashMap[String, String]
    config.put("bulk.flush.max.actions", "1")
    config.put("cluster.name", socialBDProperties.elasticClusterName)
    config.put("node.name", socialBDProperties.elasticNodeName)

//    streamTrafficKafka.addSink(new ElasticsearchSink(config, transports,
//      new SimpleElasticsearchSink(socialBDProperties.trafficConf.elasticIndex,socialBDProperties.trafficConf.elasticType)))
  }
}
