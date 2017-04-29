package ucm.socialbd.com.dataypes

/**
  * Created by Jeff on 24/04/2017.
  */
object DistributedFileModel extends DistributedFileObj{


  case class AirFile(numero: String,estacion: String, Xcoord: String,Ycoord: String,contaminantes: List[Contaminante]) extends DistributedFileObj
  case class Contaminante( magnitud: String, numMagnitud: String,unidadMedida: String,tecnica: String,numTecnica: String)

}
