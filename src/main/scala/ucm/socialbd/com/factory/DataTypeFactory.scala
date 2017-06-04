package ucm.socialbd.com.factory

import net.liftweb.json._
import twitter4j.TwitterObjectFactory
import ucm.socialbd.com.dataypes.EnrichmentFileModel.{AirFile, AirTrafficFile, TrafficFile}
import ucm.socialbd.com.dataypes.EnrichmentModel.{EAir, ETraffic, ETweet}
import ucm.socialbd.com.dataypes.RawModel.{InterUrbanTraffic, _}
import ucm.socialbd.com.dataypes.{EnrichmentFileObj, EnrichmentObj, RawObj}
import ucm.socialbd.com.utils.TwitterUtil

/**
  * Created by Jeff on 20/04/2017.
  */
object DataTypeFactory {
  private implicit val formats = net.liftweb.json.DefaultFormats

  def getRawObject(jsonString:String, ins: String): RawObj ={
    ins match {
      case Instructions.CREATE_RAW_AIR => JsonParser.parse(jsonString).extract[Air]
      case Instructions.CREATE_RAW_URBAN_TRAFFIC => JsonParser.parse(jsonString).extract[UrbanTraffic]
      case Instructions.CREATE_RAW_INTERURBAN_TRAFFIC => JsonParser.parse(jsonString).extract[InterUrbanTraffic]
      case Instructions.CREATE_RAW_TWITTER => TwitterUtil.getTwitterRawObj(jsonString)
      case Instructions.CREATE_RAW_BICIMAD => JsonParser.parse(jsonString).extract[BiciMAD]
      case Instructions.CREATE_RAW_EMTBUS => JsonParser.parse(jsonString).extract[EMTBus]
      case _ => throw new Exception(s"Cannot create Raw Object since ${ins} doesn't match")
    }
  }

  def getFileObject(jsonString:String, ins:String): EnrichmentFileObj = {
    ins match {
      case Instructions.CREATE_ENRICHMENT_FILE_AIR => JsonParser.parse(jsonString).extract[AirFile]
      case Instructions.CREATE_ENRICHMENT_FILE_TRAFFIC => JsonParser.parse(jsonString).extract[TrafficFile] //it's the same in both cases (urban and interurban)
      case Instructions.CREATE_ENRICHMENT_FILE_AIR_TRAFFIC => JsonParser.parse(jsonString).extract[AirTrafficFile]
      case _ => throw new Exception(s"Cannot create File Object since ${ins} doesn't match")
    }
  }

  def getJsonString(enrichmentObj: EnrichmentObj, ins:String): String ={
    ins match {
      case Instructions.GET_JSON_AIR => compactRender((Extraction decompose  enrichmentObj.asInstanceOf[EAir]))
      case Instructions.GET_JSON_TRAFFIC => compactRender((Extraction decompose   enrichmentObj.asInstanceOf[ETraffic]))
      case Instructions.GET_JSON_TWITTER => compactRender((Extraction decompose  enrichmentObj.asInstanceOf[ETweet]))
      case _ => throw new Exception(s"Cannot create json object since ${ins} doesn't match")
    }
  }
  def getJsonString(rawObj: RawObj, ins:String): String ={
    ins match {
      case Instructions.GET_JSON_AIR => compactRender((Extraction decompose  rawObj.asInstanceOf[Air]))
      case Instructions.GET_JSON_URBANTRAFFIC => compactRender((Extraction decompose   rawObj.asInstanceOf[UrbanTraffic]))
      case Instructions.GET_JSON_INTERURBANTRAFFIC => compactRender((Extraction decompose   rawObj.asInstanceOf[InterUrbanTraffic]))
      case Instructions.GET_JSON_TWITTER => compactRender((Extraction decompose   rawObj.asInstanceOf[Twitter]))
      case Instructions.GET_JSON_BICIMAD => compactRender((Extraction decompose   rawObj.asInstanceOf[BiciMAD]))
      case Instructions.GET_JSON_EMTBUS => compactRender((Extraction decompose rawObj.asInstanceOf[EMTBus]))
      case _ => throw new Exception(s"Cannot create json object since ${ins} doesn't match")
    }
  }
}
