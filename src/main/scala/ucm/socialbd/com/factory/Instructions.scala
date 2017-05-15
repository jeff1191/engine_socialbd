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

  val CREATE_ENRICHMENT_FILE_AIR:String = "distributedFileAir"
  val CREATE_ENRICHMENT_FILE_TRAFFIC:String = "createFileTrafficAir"
}
