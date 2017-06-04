package ucm.socialbd.com.sinks

import java.net.{InetAddress, InetSocketAddress}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.kafka.clients.producer.internals.DefaultPartitioner
import ucm.socialbd.com.config.SocialBDProperties
import ucm.socialbd.com.utils.SocialBDConfig

/**
  * Created by Jeff on 15/04/2017.
  */
class WriterSinkStream(socialBDProperties: SocialBDProperties, stream: DataStream[String]) {

  def writeDataStreamToKafka(topic:String): Unit ={
    stream.addSink(
      new FlinkKafkaProducer010[String](
        socialBDProperties.kafkaBrokersUrls,
        topic,
        new SimpleStringSchema()))
  }

  def writeDataStreamToElastic(elasticIndex: String, elasticType:String): Unit ={
    val transports = new java.util.ArrayList[InetSocketAddress]
    transports.add(new InetSocketAddress(InetAddress.getByName(socialBDProperties.elasticUrl), socialBDProperties.elasticPort))
    val config = SocialBDConfig.getElasticConfiguration(socialBDProperties)
    stream.addSink(new ElasticsearchSink(config, transports,
        new SimpleElasticsearchSink(elasticIndex, elasticType)))
  }

  def writeDataStreamToFile(path:String): Unit ={
    stream.writeAsText(path+ "/enrichment/" +
      new SimpleDateFormat("yyyyMMdd_HHmmSSS").format(new Date())).setParallelism(1)
  }
}
