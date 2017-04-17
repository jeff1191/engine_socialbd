package ucm.socialbd.com.jobs

import java.net.{InetAddress, InetSocketAddress}

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import ucm.socialbd.com.config.SocialBDProperties
import ucm.socialbd.com.sinks.SimpleElasticsearchSink
import ucm.socialbd.com.utils.SocialBDConfig

/**
  * Created by Jeff on 25/02/2017.
  */
class TwitterETL(socialBDProperties: SocialBDProperties) extends ETL{

  override def process(): Unit = {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = SocialBDConfig.getProperties(socialBDProperties)
  // Consumer group ID
  //properties.setProperty("auto.offset.reset", "earliest");

  val streamTwitterKafka: DataStream[String]= env.addSource(new FlinkKafkaConsumer010[String](
    socialBDProperties.twitterConf.twitterTopicIn, new SimpleStringSchema(), properties))
  streamTwitterKafka.print

  val transports = new java.util.ArrayList[InetSocketAddress]
  transports.add(new InetSocketAddress(InetAddress.getByName(socialBDProperties.elasticUrl), 9300))

  val config = new java.util.HashMap[String, String]
  config.put("bulk.flush.max.actions", "1")
  config.put("cluster.name", socialBDProperties.elasticClusterName)
  config.put("node.name", socialBDProperties.elasticNodeName)

  streamTwitterKafka.addSink(new ElasticsearchSink(config, transports,
    new SimpleElasticsearchSink(socialBDProperties.twitterConf.elasticIndex,socialBDProperties.twitterConf.elasticType)))
  // execute the transformation pipeline
  env.execute("Twitter Job SocialBigData-CM")
}

}
