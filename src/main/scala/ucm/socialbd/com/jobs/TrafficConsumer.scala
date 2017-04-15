package ucm.socialbd.com.jobs

import java.net.{InetAddress, InetSocketAddress}
import java.util.Properties

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
  * Created by Jeffer on 25/02/2017.
  */
object TrafficConsumer {
  private val LOCAL_ZOOKEEPER_HOST = "localhost:2181"
  private val LOCAL_KAFKA_BROKER = "localhost:9092"

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    // comma separated list of Kafka brokers
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", LOCAL_ZOOKEEPER_HOST)
    // id of the consumer group
    properties.setProperty("group.id", "test")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("key-class-type", "java.lang.String")
    properties.put("value-class-type", "java.lang.String")
    // Consumer group ID
    //properties.setProperty("auto.offset.reset", "earliest");

    val streamTwitterKafka: DataStream[String]= env.addSource(new FlinkKafkaConsumer010[String]("trafficMadrid", new SimpleStringSchema(), properties))
    streamTwitterKafka.print

    val transports = new java.util.ArrayList[InetSocketAddress]
    transports.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300))
    transports.add(new InetSocketAddress(InetAddress.getByName("localhost"), 9300))
    transports.add(new InetSocketAddress(InetAddress.getLocalHost(),9300))

    val config = new java.util.HashMap[String, String]
    config.put("bulk.flush.max.actions", "1")
    config.put("cluster.name", "social_bigdata_elastic")
    config.put("node.name", "node-1")
    //InstantiationUtil.serializeObject(streamTwitterKafka.map(x => x))
    streamTwitterKafka.addSink(new ElasticsearchSink(config, transports, new ElasticsearchSinkFunction[String] {
      def createIndexRequest(element: String): IndexRequest = {
        //Requests.indexRequest.index("tweets-spain-idx").`type`("tweets").source(copyToStringJson(element))
        Requests.indexRequest.index("traffic").`type`("traffic_type").source(element)
      }

      override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer) {
        indexer.add(createIndexRequest(element))
      }
    }))
    // execute the transformation pipeline
    env.execute("Traffic consumer SocialBigData")
  }

//  def copyToStringJson(json: String ): JsonObject ={
//    val parser = new JsonParser()
//   parser.parse(json).getAsJsonObject
//  }
}
