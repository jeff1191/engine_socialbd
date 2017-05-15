package ucm.socialbd.com.jobs

import java.net.{InetAddress, InetSocketAddress}

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import ucm.socialbd.com.config.SocialBDProperties
import ucm.socialbd.com.dataypes.EnrichmentObj
import ucm.socialbd.com.dataypes.RawModel.EMTBus
import ucm.socialbd.com.factory.Instructions
import ucm.socialbd.com.sources.KafkaFactoryConsumer
import ucm.socialbd.com.utils.SocialBDConfig

/**
  * Created by Jeff on 14/05/2017.
  */
class EMTBusETL(socialBDProperties: SocialBDProperties) extends ETL{
  override def process(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val emtBusStream: DataStream[EMTBus] = KafkaFactoryConsumer.getRawStream(env,socialBDProperties,Instructions.GET_RAW_EMTBUS).asInstanceOf[DataStream[EMTBus]]
    emtBusStream.print()
    env.execute("EMTBus Job SocialBigData-CM")
  }

  override def sendToElastic(enrDataStream: DataStream[EnrichmentObj]): Unit = {
    val transports = new java.util.ArrayList[InetSocketAddress]
    transports.add(new InetSocketAddress(InetAddress.getByName(socialBDProperties.elasticUrl), socialBDProperties.elasticPort))

    val config = new java.util.HashMap[String, String]
    config.put("bulk.flush.max.actions", "1")
    config.put("cluster.name", socialBDProperties.elasticClusterName)
    config.put("node.name", socialBDProperties.elasticNodeName)
  }
}
