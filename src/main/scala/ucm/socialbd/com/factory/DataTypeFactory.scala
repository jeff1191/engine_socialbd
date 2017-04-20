package ucm.socialbd.com.factory

import net.liftweb.json.{JsonParser, Serialization, ShortTypeHints}
import ucm.socialbd.com.dataypes.RawModel.Air

/**
  * Created by Jeff on 20/04/2017.
  */
object DataTypeFactory {

  def getRawObject(jsonString:String, ins: String): Air ={
    implicit val formats = net.liftweb.json.DefaultFormats
//    ins match {
//      case Constants.CREATE_RAW_AIR => JsonParser.parse(jsonString).extract[Air]
//      case Constants.CREATE_RAW_TRAFFIC => JsonParser.parse(jsonString).extract[Traffic]
//      case Constants.CREATE_RAW_TWITTER => JsonParser.parse(jsonString).extract[Twitter]
//      case _ => throw new Exception(s"Cannot create Raw Object since ${ins} doesn't match")
//    }


    implicit val formats2 = Serialization.formats(ShortTypeHints(List(classOf[Air])))

    val ret = JsonParser.parse(jsonString).extract[Air]
    ret
  }
}
