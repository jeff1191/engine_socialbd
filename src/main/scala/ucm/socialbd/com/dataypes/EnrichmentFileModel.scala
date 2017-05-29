package ucm.socialbd.com.dataypes

/**
  * Created by Jeff on 24/04/2017.
  */
object EnrichmentFileModel extends EnrichmentFileObj{


  case class AirFile(numero: String,estacion: String, Xcoord: String,Ycoord: String,contaminantes: List[Contaminante]) extends EnrichmentFileObj
  case class Contaminante( magnitud: String, numMagnitud: String,unidadMedida: String,tecnica: String,numTecnica: String)

  case class TrafficFile(  gid: String,
                                tipo_elem: String,
                                idelem: String,
                                cod_cent: String,
                                nombre: String,
                                Xcoord: String,
                                Ycoord: String) extends EnrichmentFileObj


  case class AirTrafficFile(estacion: String, distancia: Distancia)extends EnrichmentFileObj
  case class Distancia( sum100: List[String], //This case class have the distance between traffic station and air station, it must be sum its intensity(traffic)
                        sum1000: List[String],
                        sum250: List[String],
                        sum500: List[String])
}
