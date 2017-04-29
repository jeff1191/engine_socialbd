package ucm.socialbd.com.factory

import net.liftweb.json.{JsonParser, Serialization, ShortTypeHints}
import ucm.socialbd.com.dataypes.DistributedFileModel.AirFile
import ucm.socialbd.com.dataypes.RawModel.{Air, Traffic, Twitter}
import ucm.socialbd.com.dataypes.{DistributedFileObj, RawObj}

/**
  * Created by Jeff on 20/04/2017.
  */
object DataTypeFactory {
  implicit val formats = net.liftweb.json.DefaultFormats

  def getRawObject(jsonString:String, ins: String): RawObj ={

    ins match {
      case Instructions.CREATE_RAW_AIR => JsonParser.parse(jsonString).extract[Air]
      case Instructions.CREATE_RAW_TRAFFIC => JsonParser.parse(jsonString).extract[Traffic]
      case Instructions.CREATE_RAW_TWITTER => JsonParser.parse(jsonString).extract[Twitter]
      case _ => throw new Exception(s"Cannot create Raw Object since ${ins} doesn't match")
    }
  }

  def getFileObject(jsonString:String, ins:String):DistributedFileObj = {

    ins match {
      case Instructions.CREATE_DISTRIBUTED_FILE_AIR => JsonParser.parse(jsonString).extract[AirFile]
//      case Instructions.CREATE_DISTRIBUTED_FILE_TRAFFIC => JsonParser.parse(jsonString).extract[Traffic]
//      case Instructions.CREATE_RAW_TWITTER => JsonParser.parse(jsonString).extract[Twitter]
      case _ => throw new Exception(s"Cannot create Raw Object since ${ins} doesn't match")
    }
  }
}
