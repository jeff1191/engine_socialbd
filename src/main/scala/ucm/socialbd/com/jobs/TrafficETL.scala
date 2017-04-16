package ucm.socialbd.com.jobs

import java.net.{InetAddress, InetSocketAddress}

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import ucm.socialbd.com.config.SocialBDProperties
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

  val streamTwitterKafka: DataStream[String]= env.addSource(new FlinkKafkaConsumer010[String](
    socialBDProperties.trafficConf.trafficTopicIn, new SimpleStringSchema(), properties))
  streamTwitterKafka.print

  val transports = new java.util.ArrayList[InetSocketAddress]
  transports.add(new InetSocketAddress(InetAddress.getByName(socialBDProperties.elasticUrl), socialBDProperties.elasticPort))
//  transports.add(new InetSocketAddress(InetAddress.getByName("localhost"), 9300))
//  transports.add(new InetSocketAddress(InetAddress.getLocalHost(),9300))

  val config = new java.util.HashMap[String, String]
  config.put("bulk.flush.max.actions", "1")
  config.put("cluster.name", socialBDProperties.elasticClusterName)
  config.put("node.name", socialBDProperties.elasticNodeName)

  streamTwitterKafka.addSink(new ElasticsearchSink(config, transports, new ElasticsearchSinkFunction[String] {
    def createIndexRequest(element: String): IndexRequest = {
      Requests.indexRequest.index(socialBDProperties.trafficConf.elasticIndex).
        `type`(socialBDProperties.trafficConf.elasticType).source(element)
    }

    override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer) {
      indexer.add(createIndexRequest(element))
    }
  }))
  // execute the transformation pipeline
  env.execute("Traffic Job SocialBigData-CM")
  }
}
