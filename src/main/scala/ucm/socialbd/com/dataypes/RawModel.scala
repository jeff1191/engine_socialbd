package ucm.socialbd.com.dataypes

/**
  * Created by Jeff on 15/04/2017.
  */
object RawModel {
  case class Air(estacion: String,
                 magnitud: String,
                 tecnica: String,
                 horario: String,
                 fecha: String,
                 listaHoras: List[GroupHour]) extends RawObj
  case class GroupHour(hora:String, valor:String, isValid:String)

  case class UrbanTraffic( descripcion: String,
                            codigo: String,
                            nivelServicio: String,
                            intensidad: String,
                            intensidadSat: String,
                            accesoAsociado: String,
                            ocupacion: String,
                            subarea: String,
                            error: String,
                            carga: String,
                           timestamp:String) extends RawObj

  case class InterUrbanTraffic(codigo: String,
                                nivelServicio: String,
                                intensidad: String,
                                velocidad: String,
                                ocupacion: String,
                                error: String,
                                carga: String,
                               timestamp:String) extends RawObj

  case class Twitter (var id_str:String,
                      var createdAt:String,
                      var Xcoord:String,
                      var Ycoord:String,
                      var place:String,
                      var text:String,
                      var user:TwitterUser,
                      var retweeted:String )  extends RawObj
  case class TwitterUser(var id_str:String,
                         var followers_count:String,
                         var friends_count:String,
                         var lang:String,
                         var location:String,
                         var screenName:String)

  case class BiciMAD(address: String,
                     latitude: String,
                     no_available: String,
                     number: String,
                     light: Double,
                     name: String,
                     activate: String,
                     dock_bikes: String,
                     free_bases: String,
                     total_bases: String,
                     id: String,
                     reservations_count: String,
                     longitude: String,
                     timestamp: String) extends RawObj

  case class EMTBus(  idStop: String,
                    DescriptionStop: String,
                    Direction: String,
                    StopLine: List[StopLine],
                    ArriveEstimation: List[ArriveEstimation],
                    timestamp: String) extends RawObj
  case class StopLine(
                        Label: String,
                        Description: String
                      )
  case class ArriveEstimation(IdStop: String,
                    idLine: String,
                    IsHead: String,
                    Destination: String,
                    IdBus: String,
                    TimeLeftBus: String,
                    DistanceBus: String,
                    PositionXBus: String,
                    PositionYBus: String,
                    PositionTypeBus: String)


}
