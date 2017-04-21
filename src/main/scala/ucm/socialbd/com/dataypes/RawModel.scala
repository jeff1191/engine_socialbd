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

  case class Traffic( descripcion: String,
                      codigo: Double,
                      nivelServicio: Double,
                      intensidad: Double,
                      intensidadSat: Double,
                      accesoAsociado: Double,
                      ocupacion: Double,
                      subarea: Double,
                      error: String,
                      carga: Double) extends RawObj
  case class Twitter ()  extends RawObj


}
