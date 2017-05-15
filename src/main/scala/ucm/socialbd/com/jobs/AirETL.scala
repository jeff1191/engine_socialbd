package ucm.socialbd.com.jobs

import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.io.FilePathFilter
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import ucm.socialbd.com.config.SocialBDProperties
import ucm.socialbd.com.dataypes.EnrichmentFileModel.{AirFile, Contaminante}
import ucm.socialbd.com.dataypes.EnrichmentModel.EAir
import ucm.socialbd.com.dataypes.EnrichmentObj
import ucm.socialbd.com.dataypes.RawModel.{Air}
import ucm.socialbd.com.factory.{DataTypeFactory, Instructions}
import ucm.socialbd.com.sinks.SimpleElasticsearchSink
import ucm.socialbd.com.sources.KafkaFactoryConsumer
import ucm.socialbd.com.utils.SocialBDConfig

import scala.io.Source._
import scala.collection.mutable.ListBuffer
/**
  * Created by Jeff on 25/02/2017.
  */
class AirETL(socialBDProperties: SocialBDProperties) extends ETL{

  override def process(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val airDataStream: DataStream[Air] = KafkaFactoryConsumer.getRawStream(env,socialBDProperties,Instructions.GET_RAW_AIR).asInstanceOf[DataStream[Air]]

    val lines = fromFile(getClass.getResource("/enrichmentAirStations.json").getPath).getLines.toList
    val airFileList = lines.map(x =>  DataTypeFactory.getFileObject(x, Instructions.CREATE_ENRICHMENT_FILE_AIR).asInstanceOf[AirFile])

    //  val trafficDataStream: DataStream[Traffic] = KafkaFactoryConsumer.getRawStream(env,socialBDProperties,Instructions.GET_RAW_TRAFFIC).asInstanceOf[DataStream[Traffic]]

    val enrichmentAir = airDataStream.keyBy(_.estacion).map { rawObj =>
      val isAirElem = airFileList.filter(_.numero.equals(rawObj.estacion))

      var airFile: AirFile = AirFile("Desconocido","Desconocido","Desconocido","Desconocido",List())
      if(isAirElem.size > 0)
        airFile = isAirElem.head

      val isElem = airFile.contaminantes.filter(x => x.numMagnitud.equals(rawObj.magnitud))
      var contaminante: Contaminante = Contaminante("Magnitud desconocida", "-1", "Unidad desconocida", "Tecnica desconocida", "-1")
      if(isElem.size > 0)
        contaminante = isElem.head

      val horaElemActual = rawObj.listaHoras.filter(x => x.isValid == "V").last

      EAir(airFile.estacion, airFile.numero, airFile.Xcoord, airFile.Ycoord, rawObj.fecha + " " + horaElemActual.hora +":00",
          contaminante.magnitud, rawObj.magnitud, contaminante.tecnica, airFile.numero, horaElemActual.valor,
          "PTE")
    }
    enrichmentAir.print()
    //Enrichment DataStream
//    val joinedStream:DataStream[EAir]  =
//      airDataStream.join(airFileStream).where(_.estacion).equalTo(_.numero)
//        .window(TumblingProcessingTimeWindows.of(Time.milliseconds(5000)))
//        .apply {
//        (airRaw, airFile) =>
//          val contaminante = airFile.contaminantes.filter(x => x.numMagnitud == airRaw.magnitud).head
//          val horaElemActual = airRaw.listaHoras.filter(x => x.isValid == "V").last
//          EAir(airFile.estacion,airFile.numero,airFile.Xcoord,airFile.Ycoord,airRaw.fecha+ " "+ horaElemActual.hora,
//            contaminante.magnitud,contaminante.numMagnitud,contaminante.tecnica,contaminante.numTecnica, horaElemActual.valor,
//            "PENDIENTEINTENSIDAD")
//      }
//
//    joinedStream.print
//
//
//
//


//    airDataStream.writeAsCsv("output_air")

    // execute the transformation pipeline
    env.execute("Air Job SocialBigData-CM")
  }

  override def sendToElastic(enrDataStream: DataStream[EnrichmentObj]): Unit = {
     val transports = new java.util.ArrayList[InetSocketAddress]
    transports.add(new InetSocketAddress(InetAddress.getByName(socialBDProperties.elasticUrl), socialBDProperties.elasticPort))

    val config = new java.util.HashMap[String, String]
    config.put("bulk.flush.max.actions", "1")
    config.put("cluster.name", socialBDProperties.elasticClusterName)
    config.put("node.name", socialBDProperties.elasticNodeName)

//    streamAirKafka.addSink(new ElasticsearchSink(config, transports,
//      new SimpleElasticsearchSink(socialBDProperties.qualityAirConf.elasticIndex,socialBDProperties.qualityAirConf.elasticType)))

  }
}
