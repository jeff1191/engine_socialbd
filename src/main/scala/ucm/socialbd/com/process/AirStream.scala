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
import ucm.socialbd.com.dataypes.EnrichmentModel.{EAir, TrafficIntensity}
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
case class AuxAirTraffic (estacion:String,
                           intensidadTrafico:(String,Double),
                           distancia: List[Boolean])   //this is a aux class for to help us enrichment Air with Traffic

case class CrossAirTraffic(idStation:String, traffic100m:Double, traffic250m:Double, traffic500m:Double, traffic1000m:Double)

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


    val dataStreamMeanTraffic:DataStream[AuxAirTraffic] = dataStreamMeanUTraffic.union(dataStreamMeanITraffic).map{elem =>

      val elemList100 = airTrafficList.filter(x => x.distancia.sum100.exists(x => x.equalsIgnoreCase(elem._1))).map( x => x.estacion)
      val elemList250 = airTrafficList.filter(x => x.distancia.sum250.exists(x => x.equalsIgnoreCase(elem._1))).map( x => x.estacion)
      val elemList500 = airTrafficList.filter(x => x.distancia.sum500.exists(x => x.equalsIgnoreCase(elem._1))).map( x => x.estacion)
      val elemList1000 = airTrafficList.filter(x => x.distancia.sum1000.exists(x => x.equalsIgnoreCase(elem._1))).map( x => x.estacion)

      var estacion = "-1"
      var elem100 = false
      var elem250 =false
      var elem500 =false
      var elem1000 =false

      if(elemList100.nonEmpty){
        estacion = elemList100.head
        elem100 = true
      }
      if(elemList250.nonEmpty){
        estacion = elemList250.head
        elem250 = true
      }
      if(elemList500.nonEmpty){
        estacion = elemList500.head
        elem500 = true
      }
      if(elemList1000.nonEmpty){
        estacion = elemList1000.head
        elem1000 =true
      }

      AuxAirTraffic(estacion,elem, List(elem100,elem250,elem500,elem1000)) //this return for instance: (AirTrafficStation, (TrafficStationCod,meanIntensity), List( if traffic station is a x-meters from air station)
    }
    dataStreamMeanTraffic.writeAsText("data/dd/pepe").setParallelism(1)
    //In this point we need to sum the traffic intensity where air stations are the same and considering the distance (100m, 250m, 500m, 1000m), the traffic station it doesn't matter
    val dataStreamMeanTraffic100m = dataStreamMeanTraffic.filter(x => x.distancia(0) && !x.distancia(1) && !x.distancia(2)&& !x.distancia(3)).keyBy(x => x.estacion)
      .reduce { (x, y) =>
        AuxAirTraffic(x.estacion,(x.intensidadTrafico._1, x.intensidadTrafico._2 + y.intensidadTrafico._2), x.distancia)
      }.writeAsText("data/pp100m"+ "/enrichment/" +
      new SimpleDateFormat("yyyyMMdd_HHmmSSS").format(new Date())).setParallelism(1)
    val dataStreamMeanTraffic250m = dataStreamMeanTraffic.filter(x => !x.distancia(0)&& x.distancia(1) && !x.distancia(2)&& !x.distancia(3)).keyBy(x => x.estacion)
      .reduce { (x, y) =>
        AuxAirTraffic(x.estacion,(x.intensidadTrafico._1, x.intensidadTrafico._2 + y.intensidadTrafico._2), x.distancia)
      }.writeAsText("data/pp250m"+ "/enrichment/" +
      new SimpleDateFormat("yyyyMMdd_HHmmSSS").format(new Date())).setParallelism(1)
    val dataStreamMeanTraffic500m = dataStreamMeanTraffic.filter(x => !x.distancia(0)&& !x.distancia(1) && x.distancia(2)&& !x.distancia(3)).keyBy(x => x.estacion)
      .reduce { (x, y) =>
        AuxAirTraffic(x.estacion,(x.intensidadTrafico._1, x.intensidadTrafico._2 + y.intensidadTrafico._2), x.distancia)
      }.writeAsText("data/pp500m"+ "/enrichment/" +
      new SimpleDateFormat("yyyyMMdd_HHmmSSS").format(new Date())).setParallelism(1)
    val dataStreamMeanTraffic1000m = dataStreamMeanTraffic.filter(x => !x.distancia(0)&& !x.distancia(1) && !x.distancia(2)&& x.distancia(3)).keyBy(x => x.estacion)
      .reduce { (x, y) =>
        AuxAirTraffic(x.estacion,(x.intensidadTrafico._1, x.intensidadTrafico._2 + y.intensidadTrafico._2), x.distancia)
      }.writeAsText("data/pp1000m"+ "/enrichment/" +
      new SimpleDateFormat("yyyyMMdd_HHmmSSS").format(new Date())).setParallelism(1)

    //Now we need to join the different dataStreams for to get up an unique dataStream like (AirStation, [(intensity,100m),(intensity,200m))....etc]
//    val crossDataStreamAirTraffic = dataStreamMeanTraffic1000m.join(dataStreamMeanTraffic500m).where(_.estacion).equalTo(_.estacion)
//          .window(TumblingProcessingTimeWindows.of(Time.milliseconds(10)))
//          .apply {
//            (rightObj1000m, leftObj500m) =>
//              CrossAirTraffic(rightObj1000m.estacion,-1,-1,
//                leftObj500m.intensidadTrafico._2,rightObj1000m.intensidadTrafico._2)
//          }.join(dataStreamMeanTraffic250m).where(_.idStation).equalTo(_.estacion)
//          .window(TumblingProcessingTimeWindows.of(Time.milliseconds(10)))
//            .apply {
//              (rightObj, leftObj250m) =>
//                CrossAirTraffic(rightObj.idStation,-1,leftObj250m.intensidadTrafico._2,
//                  rightObj.traffic500m,rightObj.traffic1000m)
//         }.join(dataStreamMeanTraffic100m).where(_.idStation).equalTo(_.estacion)
//          .window(TumblingProcessingTimeWindows.of(Time.milliseconds(10)))
//          .apply {
//            (rightObj, leftObj100m) =>
//              CrossAirTraffic(rightObj.idStation,leftObj100m.intensidadTrafico._2,rightObj.traffic250m,
//                rightObj.traffic500m,rightObj.traffic1000m)
//          }.filter(x => !x.idStation.equals("-1")).keyBy(x => x.idStation).map(x => x).writeAsText("data/pp"+ "/enrichment/" +
//      new SimpleDateFormat("yyyyMMdd_HHmmSSS").format(new Date())).setParallelism(1)

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
//      val defIntensityList = airTrafficEnrichmentFile.filter(elem => elem._1.equals(rawObj.estacion)).map(elem => elem._2).flatMap(x => x)

      EAir(airFile.estacion, airFile.numero, airFile.Xcoord, airFile.Ycoord, rawObj.fecha + " " + horaElemActual.hora + ":00",
        contaminante.magnitud, rawObj.magnitud, contaminante.tecnica, airFile.numero, horaElemActual.valor,
        null)
    }

//    crossDataStreamAirTraffic.print()
    enrichmentAir.print()








    //Enrichment final datastream

//    val joinedStream =
//    enrichmentAir.join(dataStreamMeanTraffic).where(_.estacion).equalTo(_._1)
//      .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
//      .apply {
//        (enrichObj, airTrafficObj) =>
//          (enrichObj.nivelIntensidadTrafico, airTrafficObj._2.toString)
//      }
//    joinedStream.print()
//    val jsonEnrDataStream  = joinedStream
//      .map(enrObj => DataTypeFactory.getJsonString(enrObj, Instructions.GET_JSON_AIR).toString)
//    val jsonRawDataStream  = airDataStream
//        .map(enrObj => DataTypeFactory.getJsonString(enrObj, Instructions.GET_JSON_AIR).toString)
//        .writeAsText(socialBDProperties.qualityAirConf.outputDir+ "/raw/" + new SimpleDateFormat("yyyyMMdd_HHmmSSS").format(new Date()))
//        .setParallelism(1)
//    writeDataStreamToSinks(jsonEnrDataStream)
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


  private def getIntensityList(estacion: String, env:StreamExecutionEnvironment): List[TrafficIntensity] ={

null

  }
}
