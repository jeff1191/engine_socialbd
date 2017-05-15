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

}
