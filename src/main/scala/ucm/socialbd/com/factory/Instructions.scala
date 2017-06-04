package ucm.socialbd.com.factory

/**
  * Created by Jeff on 20/04/2017.
  */
object Instructions {
  val CREATE_RAW_AIR:String = "createRawAir"
  val CREATE_RAW_URBAN_TRAFFIC:String = "createRawUrbanTraffic"
  val CREATE_RAW_INTERURBAN_TRAFFIC:String = "createRawInterUrbanTraffic"
  val CREATE_RAW_TWITTER:String = "createRawTwitter"
  val CREATE_RAW_BICIMAD:String = "createRawBiciMAD"
  val CREATE_RAW_EMTBUS:String = "createRawEMTBus"

  val GET_RAW_AIR: String = "getRawAir"
  val GET_RAW_URBAN_TRAFFIC: String = "getRawUrbanTraffic"
  val GET_RAW_INTERURBAN_TRAFFIC: String = "getRawInterUrbanTraffic"
  val GET_RAW_TWITTER: String = "getRawTwitter"
  val GET_RAW_BICIMAD: String = "getRawBiciMAD"
  val GET_RAW_EMTBUS: String = "getRawEMTBus"

  val CREATE_ENRICHMENT_FILE_AIR:String = "createdFileAir"
  val CREATE_ENRICHMENT_FILE_TRAFFIC:String = "createFileTraffic"
  val CREATE_ENRICHMENT_FILE_AIR_TRAFFIC:String = "createFileTrafficAir"

  val GET_JSON_AIR: String = "getJsonAir"
  val GET_JSON_TRAFFIC: String = "getJsonTraffic"
  val GET_JSON_URBANTRAFFIC: String = "getJsonUrbanTraffic"
  val GET_JSON_INTERURBANTRAFFIC: String = "getJsonInterUrbanTraffic"
  val GET_JSON_TWITTER: String = "getJsonTwitter"
  val GET_JSON_BICIMAD: String = "getJsonBiciMAD"
  val GET_JSON_EMTBUS: String = "getJsonEMTBus"

  val SAVE_ENRICHMENT_AIR = "saveAirElastic"
  val SAVE_ENRICHMENT_TRAFFIC = "saveAirTraffic"
  val SAVE_ENRICHMENT_TWITTER = "saveAirTwitter"
  val SAVE_ENRICHMENT_BICIMAD = "saveAirTwitter"
  val SAVE_ENRICHMENT_EMTBUS = "saveAirTwitter"

  val SAVE_RAW_AIR = "saveAirElastic"
  val SAVE_RAW_TRAFFIC = "saveAirTraffic"
  val SAVE_RAW__TWITTER = "saveAirTwitter"
  val SAVE_RAW__BICIMAD = "saveAirBiciMad"
  val SAVE_RAW__EMTBUS = "saveEmtBus"
}
