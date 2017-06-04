package ucm.socialbd.com.process

import java.net.{InetAddress, InetSocketAddress}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.util.Collector
import ucm.socialbd.com.config.SocialBDProperties
import ucm.socialbd.com.dataypes.EnrichmentFileModel.{AirFile, AirTrafficFile, Contaminante}
import ucm.socialbd.com.dataypes.EnrichmentModel.{EAir}
import ucm.socialbd.com.dataypes.{EnrichmentObj, RawObj}
import ucm.socialbd.com.dataypes.RawModel.{Air, InterUrbanTraffic, UrbanTraffic}
import ucm.socialbd.com.factory.{DataTypeFactory, Instructions}
import ucm.socialbd.com.sinks.{SimpleElasticsearchSink, WriterSinkStream}
import ucm.socialbd.com.sources.KafkaFactoryConsumer

import scala.io.Source._
import scala.collection.mutable.ListBuffer
/**
  * Created by Jeff on 25/02/2017.
  */


class AirStream(socialBDProperties: SocialBDProperties) extends StreamTransform {


  override def process(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val airDataStream: DataStream[Air] = KafkaFactoryConsumer.getRawStream(env, socialBDProperties, Instructions.GET_RAW_AIR).asInstanceOf[DataStream[Air]]

    val airFileList = fromFile(getClass.getResource("/enrichmentAirStations.json").getPath).getLines.toList
      .map(x => DataTypeFactory.getFileObject(x, Instructions.CREATE_ENRICHMENT_FILE_AIR).asInstanceOf[AirFile])

    val airTrafficList = fromFile(getClass.getResource("/enrichmentAirTraffic.json").getPath).getLines.toList
      .map(x => DataTypeFactory.getFileObject(x, Instructions.CREATE_ENRICHMENT_FILE_AIR_TRAFFIC).asInstanceOf[AirTrafficFile])
    //Calculate intensity mean about traffic source for enrichment air

    val urbanTrafficDataStream: DataStream[UrbanTraffic] = KafkaFactoryConsumer.getRawStream(env, socialBDProperties, Instructions.GET_RAW_URBAN_TRAFFIC).asInstanceOf[DataStream[UrbanTraffic]]
    val interUrbanTrafficDataStream: DataStream[InterUrbanTraffic] = KafkaFactoryConsumer.getRawStream(env, socialBDProperties, Instructions.GET_RAW_INTERURBAN_TRAFFIC).asInstanceOf[DataStream[InterUrbanTraffic]]

    val dataStreamMeanUTraffic = urbanTrafficDataStream.keyBy(_.codigo).timeWindow(Time.minutes(5), Time.seconds(10)).
      apply((key, window, input, out: Collector[(String, Double)]) => {
        val dataStream = input.filter(!_.intensidad.equals("")).map(x => x.intensidad.toInt)
        val sum = dataStream.reduce((x, y) => x + y)
        out.collect(key, sum / input.size)
      })

    val dataStreamMeanITraffic = interUrbanTrafficDataStream.keyBy(_.codigo).timeWindow(Time.minutes(5), Time.seconds(1)).
      apply((key, window, input, out: Collector[(String, Double)]) => {
        val dataStream = input.filter(!_.intensidad.equals("")).map(x => x.intensidad.toInt)
        val sum = dataStream.reduce((x, y) => x + y)
        out.collect(key, sum / input.size)
      })


    val dataStreamMeanTraffic = dataStreamMeanUTraffic.union(dataStreamMeanITraffic).map{elem =>

      val elemList1000 = airTrafficList.filter(x => x.distancia.sum1000.exists(x => x.equalsIgnoreCase(elem._1))).map( x => x.estacion)
      var estacion = "-1"
      if(elemList1000.nonEmpty){
        estacion = elemList1000.head
      }

        (estacion,elem) //we need group for station but without lost it in the keyBy()
    }

//    val dataStreamAirTrafficDef2 = dataStreamMeanTraffic.keyBy(_._1).reduce((x,y) => (x._1,(x._2._1, x._2._2 + y._2._2)))

    val dataStreamAirTrafficDef = dataStreamMeanTraffic.filter(_._1 != "-1").keyBy(_._1).timeWindow(Time.minutes(5), Time.seconds(1)).
      apply((key, window, input, out: Collector[(String, Double)]) => {
        val sum = input.map(x => x._2._2).sum
        out.collect(key, sum / input.size)
      })

    //    Enrichment final datastream
    val enrichmentAir = airDataStream.keyBy(_.estacion).join(dataStreamAirTrafficDef).where(_.estacion).equalTo(_._1)
          .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
          .apply { //!!There are contaminantes unknowns¡¡
            (leftObj, rightObj) =>

              val isAirElem = airFileList.filter(_.numero.equals(leftObj.estacion))

              var airFile: AirFile = AirFile("Unknown", "Unknown", "Unknown", "Unknown", List())
              if (isAirElem.size > 0)
                airFile = isAirElem.head

              val isElem = airFile.contaminantes.filter(x => x.numMagnitud.equals(leftObj.magnitud))
              var contaminante: Contaminante = Contaminante("Unknown", "-1", "Unknown", "Unknown", "-1")
              if (isElem.size > 0)
                contaminante = isElem.head

              val horaElemActual = leftObj.listaHoras.filter(x => x.isValid == "V").last

              EAir(airFile.estacion, airFile.numero, airFile.Xcoord, airFile.Ycoord, leftObj.fecha + " " + horaElemActual.hora + ":00",
                contaminante.magnitud, leftObj.magnitud, contaminante.tecnica, airFile.numero, horaElemActual.valor,
                rightObj._2)

          }


    val jsonEnrDataStream  = enrichmentAir
      .map(enrObj => DataTypeFactory.getJsonString(enrObj, Instructions.GET_JSON_AIR).toString)
    val jsonRawDataStream  = airDataStream
        .map(enrObj => DataTypeFactory.getJsonString(enrObj, Instructions.GET_JSON_AIR).toString)
        .writeAsText(socialBDProperties.qualityAirConf.outputDir+ "/raw/" + new SimpleDateFormat("yyyyMMdd_HHmmSSS").format(new Date()))
        .setParallelism(1)
    writeDataStreamToSinks(jsonEnrDataStream)
    // execute the transformation pipeline
    env.execute("Air Job SocialBigData-CM")
  }

  override def writeDataStreamToSinks(enrDataStream: DataStream[String]): Unit = {
    val writer = new WriterSinkStream(socialBDProperties ,enrDataStream)

    socialBDProperties.outputMode.toUpperCase.split(",").foreach(_.trim match {
      case "KAFKA" => writer.writeDataStreamToKafka(socialBDProperties.qualityAirConf.qualityAirTopicOut)
      case "FILE" =>  writer.writeDataStreamToFile(socialBDProperties.qualityAirConf.outputDir)
      case "ELASTIC" => writer.writeDataStreamToElastic(socialBDProperties.qualityAirConf.elasticIndex,socialBDProperties.qualityAirConf.elasticType)
    })
  }

}
