package ucm.socialbd.com.factory

import net.liftweb.json.{JsonParser, Serialization, ShortTypeHints}
import ucm.socialbd.com.dataypes.EnrichmentFileModel.{AirFile, TrafficFile}
import ucm.socialbd.com.dataypes.RawModel._
import ucm.socialbd.com.dataypes.{EnrichmentFileObj, RawObj}

/**
  * Created by Jeff on 20/04/2017.
  */
object DataTypeFactory {
  implicit val formats = net.liftweb.json.DefaultFormats

  def getRawObject(jsonString:String, ins: String): RawObj ={
    ins match {
      case Instructions.CREATE_RAW_AIR => JsonParser.parse(jsonString).extract[Air]
      case Instructions.CREATE_RAW_URBAN_TRAFFIC => JsonParser.parse(jsonString).extract[UrbanTraffic]
      case Instructions.CREATE_RAW_INTERURBAN_TRAFFIC => JsonParser.parse(jsonString).extract[InterUrbanTraffic]
      case Instructions.CREATE_RAW_TWITTER => JsonParser.parse(jsonString).extract[Twitter]
      case Instructions.CREATE_RAW_BICIMAD => JsonParser.parse(jsonString).extract[BiciMAD]
      case Instructions.CREATE_RAW_EMTBUS => JsonParser.parse(jsonString).extract[EMTBus]
      case _ => throw new Exception(s"Cannot create Raw Object since ${ins} doesn't match")
    }
  }

  def getFileObject(jsonString:String, ins:String): EnrichmentFileObj = {
    ins match {
      case Instructions.CREATE_ENRICHMENT_FILE_AIR => JsonParser.parse(jsonString).extract[AirFile]
      case Instructions.CREATE_ENRICHMENT_FILE_TRAFFIC => JsonParser.parse(jsonString).extract[TrafficFile]
      case _ => throw new Exception(s"Cannot create File Object since ${ins} doesn't match")
    }
  }
}
