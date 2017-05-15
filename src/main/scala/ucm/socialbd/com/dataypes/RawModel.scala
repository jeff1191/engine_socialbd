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

  case class Twitter ()  extends RawObj

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

  case class EMTBus(  Line: String,
                      OrderDetail: String,
                      Node: String,
                      DistStopPrev: String,
                      PosxNode: String,
                      PosyNode: String,
                      SecDetail: String,
                      Distance: String,
                      Name: String,
                      timestamp: String) extends RawObj
}
