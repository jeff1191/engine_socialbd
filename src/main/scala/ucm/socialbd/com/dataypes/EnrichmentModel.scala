package ucm.socialbd.com.dataypes

/**
  * Created by Jeff on 15/04/2017.
  */
object EnrichmentModel {
  case class ETweet() extends EnrichmentObj
  case class EAir(estacion: String,codigoEst: String, Xcoord: String, Ycoord: String, fechaHora: String,
                  magnitudNombre: String,magnitudCod: String, tecnicaNom: String,
                  tecnicaCod: String,valor: String, nivelIntensidadTrafico: String) extends EnrichmentObj
  case class ETraffic()extends EnrichmentObj
}
