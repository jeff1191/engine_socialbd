package ucm.socialbd.com.jobs

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import ucm.socialbd.com.config.SocialBDProperties
import ucm.socialbd.com.dataypes.RawModel.Air
import ucm.socialbd.com.factory.{Constants, DataTypeFactory}
import ucm.socialbd.com.utils.SocialBDConfig
/**
  * Created by Jeff on 25/02/2017.
  */
class AirETL(socialBDProperties: SocialBDProperties) extends ETL{

  override def process(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = SocialBDConfig.getProperties(socialBDProperties)
    // Consumer group ID
    //properties.setProperty("auto.offset.reset", "earliest");

    val streamAirKafka: DataStream[String]= env.addSource(
      new FlinkKafkaConsumer010[String](socialBDProperties.qualityAirConf.qualityAirTopicIn, new SimpleStringSchema(), properties))

    val airElems: DataStream[Air] = streamAirKafka.map(x => DataTypeFactory.getRawObject(x,Constants.CREATE_RAW_AIR).asInstanceOf[Air])

    airElems.map( x => x.estacion).print()


    /*
     val transports = new java.util.ArrayList[InetSocketAddress]
    transports.add(new InetSocketAddress(InetAddress.getByName(socialBDProperties.elasticUrl), socialBDProperties.elasticPort))

    val config = new java.util.HashMap[String, String]
    config.put("bulk.flush.max.actions", "1")
    config.put("cluster.name", socialBDProperties.elasticClusterName)
    config.put("node.name", socialBDProperties.elasticNodeName)

    streamAirKafka.addSink(new ElasticsearchSink(config, transports,
      new SimpleElasticsearchSink(socialBDProperties.qualityAirConf.elasticIndex,socialBDProperties.qualityAirConf.elasticType)))
    */
    // execute the transformation pipeline
    env.execute("Air Job SocialBigData-CM")
  }
}
