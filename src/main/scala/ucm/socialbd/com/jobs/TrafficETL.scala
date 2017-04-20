package ucm.socialbd.com.jobs

import java.net.{InetAddress, InetSocketAddress}

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import ucm.socialbd.com.config.SocialBDProperties
import ucm.socialbd.com.sinks.SimpleElasticsearchSink
import ucm.socialbd.com.utils.SocialBDConfig

/**
  * Created by Jeff on 25/02/2017.
  */
class TrafficETL(socialBDProperties: SocialBDProperties) extends ETL{

  override def process(): Unit = {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val properties = SocialBDConfig.getProperties(socialBDProperties)
  // Consumer group ID
  //properties.setProperty("auto.offset.reset", "earliest");

  val streamTrafficKafka: DataStream[String]= env.addSource(new FlinkKafkaConsumer010[String](
    socialBDProperties.trafficConf.trafficTopicIn, new SimpleStringSchema(), properties))
    streamTrafficKafka.print

  val transports = new java.util.ArrayList[InetSocketAddress]
  transports.add(new InetSocketAddress(InetAddress.getByName(socialBDProperties.elasticUrl), socialBDProperties.elasticPort))

  val config = new java.util.HashMap[String, String]
  config.put("bulk.flush.max.actions", "1")
  config.put("cluster.name", socialBDProperties.elasticClusterName)
  config.put("node.name", socialBDProperties.elasticNodeName)

  streamTrafficKafka.addSink(new ElasticsearchSink(config, transports,
  new SimpleElasticsearchSink(socialBDProperties.trafficConf.elasticIndex,socialBDProperties.trafficConf.elasticType)))
  // execute the transformation pipeline
  env.execute("Traffic Job SocialBigData-CM")
  }
}