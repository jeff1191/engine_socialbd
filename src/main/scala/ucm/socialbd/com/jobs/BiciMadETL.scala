package ucm.socialbd.com.jobs

import java.net.{InetAddress, InetSocketAddress}

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import ucm.socialbd.com.config.SocialBDProperties
import ucm.socialbd.com.dataypes.EnrichmentObj
import ucm.socialbd.com.dataypes.RawModel.BiciMAD
import ucm.socialbd.com.factory.Instructions
import ucm.socialbd.com.sources.KafkaFactoryConsumer
import ucm.socialbd.com.utils.SocialBDConfig

/**
  * Created by Jeff on 14/05/2017.
  */
class BiciMadETL(socialBDProperties: SocialBDProperties) extends ETL{
  override def process(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val biciDataStream: DataStream[BiciMAD] = KafkaFactoryConsumer.getRawStream(env,socialBDProperties,Instructions.GET_RAW_BICIMAD).asInstanceOf[DataStream[BiciMAD]]
    biciDataStream.print()
    env.execute("BiciMad Job SocialBigData-CM")
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
