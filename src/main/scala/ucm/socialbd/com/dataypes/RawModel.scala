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
                 lista: List[GroupHour])
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
                      carga: Double) extends RawObject
  case class Twitter ()  extends RawObject


}
