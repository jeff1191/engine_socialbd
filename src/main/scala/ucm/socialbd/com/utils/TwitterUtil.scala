package ucm.socialbd.com.utils

import net.liftweb.json
import twitter4j.{Status, TwitterObjectFactory}
import ucm.socialbd.com.dataypes.RawModel.{Twitter, TwitterUser}
import net.liftweb.json._
/**
  * Created by Jeff on 21/05/2017.
  */
object TwitterUtil {

  // TODO: this method must be edited, now it works basically
  def getTwitterRawObj(rawJSON:String): Twitter ={

    //BUG : createStatus it doesn't be able to convert all strings in Status Object, therefore need us use json.liftweb utils
    val status = TwitterObjectFactory.createStatus(rawJSON) // <- this don't work correctly
    val jsonMap = parse(rawJSON)

    val rawTwitterObj =Twitter("Unknown","Unknown","Unknown",
      "Unknown","Unknown","Unknown",
      TwitterUser("Unknown","Unknown","Unknown","Unknown",
        "Unknown","Unknown"),
      "Unknown")

    rawTwitterObj.id_str = status.getId.toString
    rawTwitterObj.place = compactRender(jsonMap \ "place" \ "fullName").toString

    rawTwitterObj.createdAt = compactRender(jsonMap \ "createdAt").toString
    if(status.getGeoLocation != null)
      rawTwitterObj.Xcoord = status.getGeoLocation.getLatitude.toString
    if(status.getGeoLocation != null)
      rawTwitterObj.Ycoord = status.getGeoLocation.getLongitude.toString

    if(status.getText != null)
      rawTwitterObj.text = status.getText
    if(status.getUser != null)
      rawTwitterObj.user = TwitterUser(status.getUser.getId.toString,status.getUser.getFollowersCount.toString,status.getUser.getFriendsCount.toString,
        status.getUser.getLang,status.getUser.getLocation,status.getUser.getScreenName)

    rawTwitterObj.retweeted =  compactRender(jsonMap \ "retweetCount" ).toString
    rawTwitterObj
  }
}
