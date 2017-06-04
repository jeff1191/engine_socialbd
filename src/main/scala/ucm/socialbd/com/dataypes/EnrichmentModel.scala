package ucm.socialbd.com.dataypes

/**
  * Created by Jeff on 15/04/2017.
  */
object EnrichmentModel {
  case class EAir(estacion: String,
                  codigoEst: String,
                  Xcoord: String,
                  Ycoord: String,
                  fechaHora: String,
                  magnitudNombre: String,
                  magnitudCod: String,
                  tecnicaNom: String,
                  tecnicaCod: String,
                  valor: String, nivelIntensidadTrafico: List[TrafficIntensity]) extends EnrichmentObj
  case class TrafficIntensity(distancia:String, intensidad:Double) //<- distancia means if this intensity is 100m,250m,500m,1000m
                                                                    //intensidad represents the sum of traffic stations near air station

  case class ETraffic(idelem : String,
                      identif : String,
                      nombre: String,
                      fechaHora: String,
                      tipo_elem : String,
                      intensidad : String,
                      carga : String,
                      vmed : String,
                      error : String,
                      Xcoord : String,
                      Ycoord : String)extends EnrichmentObj

  case class ETweet() extends EnrichmentObj

}
