package ucm.socialbd.com.process

import java.net.{InetAddress, InetSocketAddress}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.io.FilePathFilter
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010.FlinkKafkaProducer010Configuration
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import ucm.socialbd.com.config.SocialBDProperties
import ucm.socialbd.com.dataypes.EnrichmentFileModel.{AirFile, AirTrafficFile, Contaminante}
import ucm.socialbd.com.dataypes.EnrichmentModel.EAir
import ucm.socialbd.com.dataypes.{EnrichmentObj, RawObj}
import ucm.socialbd.com.dataypes.RawModel.{Air, InterUrbanTraffic, UrbanTraffic}
import ucm.socialbd.com.factory.{DataTypeFactory, Instructions}
import ucm.socialbd.com.sinks.{SimpleElasticsearchSink, WriterSinkStream}
import ucm.socialbd.com.sources.KafkaFactoryConsumer
import ucm.socialbd.com.utils.SocialBDConfig

import scala.io.Source._
import scala.collection.mutable.ListBuffer
/**
  * Created by Jeff on 25/02/2017.
  */
class AirStream(socialBDProperties: SocialBDProperties) extends StreamTransform {

  override def process(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val airDataStream: DataStream[Air] = KafkaFactoryConsumer.getRawStream(env, socialBDProperties, Instructions.GET_RAW_AIR).asInstanceOf[DataStream[Air]]

    val lines = fromFile(getClass.getResource("/enrichmentAirStations.json").getPath).getLines.toList
    val airFileList = lines.map(x => DataTypeFactory.getFileObject(x, Instructions.CREATE_ENRICHMENT_FILE_AIR).asInstanceOf[AirFile])

    val enrichmentAir = airDataStream.keyBy(_.estacion).map { rawObj => //There are contaminantes unknowns and fields unknowns to enrichment it
      val isAirElem = airFileList.filter(_.numero.equals(rawObj.estacion))

      var airFile: AirFile = AirFile("Unknown", "Unknown", "Unknown", "Unknown", List())
      if (isAirElem.size > 0)
        airFile = isAirElem.head

      val isElem = airFile.contaminantes.filter(x => x.numMagnitud.equals(rawObj.magnitud))
      var contaminante: Contaminante = Contaminante("Unknown", "-1", "Unknown", "Unknown", "-1")
      if (isElem.size > 0)
        contaminante = isElem.head

      val horaElemActual = rawObj.listaHoras.filter(x => x.isValid == "V").last

      EAir(airFile.estacion, airFile.numero, airFile.Xcoord, airFile.Ycoord, rawObj.fecha + " " + horaElemActual.hora + ":00",
        contaminante.magnitud, rawObj.magnitud, contaminante.tecnica, airFile.numero, horaElemActual.valor,
        "PTE")
    }

    val airTrafficList = fromFile(getClass.getResource("/enrichmentAirTraffic.json").getPath).getLines.toList
      .map(x => DataTypeFactory.getFileObject(x, Instructions.CREATE_ENRICHMENT_FILE_AIR_TRAFFIC).asInstanceOf[AirTrafficFile])

    val urbanTrafficDataStream: DataStream[UrbanTraffic] = KafkaFactoryConsumer.getRawStream(env, socialBDProperties, Instructions.GET_RAW_URBAN_TRAFFIC).asInstanceOf[DataStream[UrbanTraffic]]
    val interUrbanTrafficDataStream: DataStream[InterUrbanTraffic] = KafkaFactoryConsumer.getRawStream(env, socialBDProperties, Instructions.GET_RAW_INTERURBAN_TRAFFIC).asInstanceOf[DataStream[InterUrbanTraffic]]

    val dataStreamMeanUTraffic = urbanTrafficDataStream.keyBy(_.codigo).timeWindow(Time.minutes(1), Time.seconds(10)).
      apply((key, window, input, out: Collector[(String, Double)]) => {
        val dataStream = input.filter(!_.intensidad.equals("")).map(x => x.intensidad.toInt)
        val sum = dataStream.reduce((x, y) => x + y)
        out.collect(key, sum / input.size)
      })

    val dataStreamMeanITraffic = interUrbanTrafficDataStream.keyBy(_.codigo).timeWindow(Time.minutes(5), Time.minutes(1)).
      apply((key, window, input, out: Collector[(String, Double)]) => {
        val dataStream = input.filter(!_.intensidad.equals("")).map(x => x.intensidad.toInt)
        val sum = dataStream.reduce((x, y) => x + y)
        out.collect(key, sum / input.size)
      })
    val dataStreamMeanTraffic = dataStreamMeanUTraffic.union(dataStreamMeanITraffic)

    //Enrichment final datastream
    val joinedStream: DataStream[EAir] =
    enrichmentAir.join(dataStreamMeanTraffic).where(_.estacion).equalTo(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.milliseconds(5000)))
      .apply {
        (enrichObj, airTrafficObj) =>
          enrichObj.copy(nivelIntensidadTrafico = airTrafficObj._2.toString)
      }

    val jsonDataStream  = joinedStream
      .map(enrObj => DataTypeFactory.getJsonString(enrObj, Instructions.GET_JSON_AIR).toString)

    writeDataStreamToSinks(jsonDataStream)
    // execute the transformation pipeline
    env.execute("Air Job SocialBigData-CM")
  }

  override def writeDataStreamToSinks(enrDataStream: DataStream[String]): Unit = {
    val writer = new WriterSinkStream(socialBDProperties ,enrDataStream)

    socialBDProperties.outputMode.toUpperCase.split(",").foreach(_.trim match {
      case "KAFKA" => writer.writeDataStreamToKafka(socialBDProperties.qualityAirConf.qualityAirTopicOut)
      case "FILE" => writer.writeDataStreamToFile(socialBDProperties.qualityAirConf.outputDir)
      case "ELASTIC" => writer.writeDataStreamToElastic(socialBDProperties.qualityAirConf.elasticIndex,socialBDProperties.qualityAirConf.elasticType)
    })
  }
}
