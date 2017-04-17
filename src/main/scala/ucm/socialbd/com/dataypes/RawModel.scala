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
                 hora: String,
                 isValid: String)
  case class Traffic( descripcion: String,
                      codigo: Double,
                      nivelServicio: Double,
                      intensidad: Double,
                      intensidadSat: Double,
                      accesoAsociado: Double,
                      ocupacion: Double,
                      subarea: Double,
                      error: String,
                      carga: Double)

  case class User_mentions(
                            name: String,
                            id_str: String,
                            id: Double,
                            indices: List[Double],
                            screen_name: String
                          )

  case class Urls(
                   expanded_url: String,
                   url: String,
                   indices: List[Double]
                 )
  case class Url(
                  urls: List[Urls]
                )
  case class Entities(
                       url: Url,
                       description: Url
                     )
  case class User(
                   profile_sidebar_fill_color: String,
                   profile_sidebar_border_color: String,
                   profile_background_tile: Boolean,
                   name: String,
                   profile_image_url: String,
                   created_at: String,
                   location: String,
                   follow_request_sent: Boolean,
                   profile_link_color: String,
                   is_translator: Boolean,
                   id_str: String,
                   entities: Entities,
                   default_profile: Boolean,
                   contributors_enabled: Boolean,
                   favourites_count: Double,
                   url: String,
                   profile_image_url_https: String,
                   utc_offset: Double,
                   id: Double,
                   profile_use_background_image: Boolean,
                   listed_count: Double,
                   profile_text_color: String,
                   lang: String,
                   followers_count: Double,
                   `protected`: Boolean,
                   notifications: String,
                   profile_background_image_url_https: String,
                   profile_background_color: String,
                   verified: Boolean,
                   geo_enabled: Boolean,
                   time_zone: String,
                   description: String,
                   default_profile_image: Boolean,
                   profile_background_image_url: String,
                   statuses_count: Double,
                   friends_count: Double,
                   following: String,
                   show_all_inline_media: Boolean,
                   screen_name: String
                 )
  case class Tweet(
                             coordinates: String,
                             favorited: Boolean,
                             truncated: Boolean,
                             created_at: String,
                             id_str: String,
                             entities: Entities,
                             in_reply_to_user_id_str: String,
                             contributors: String,
                             text: String,
                             retweet_count: Double,
                             in_reply_to_status_id_str: String,
                             id: Double,
                             geo: String,
                             retweeted: Boolean,
                             in_reply_to_user_id: Double,
                             place: String,
                             user: User,
                             in_reply_to_screen_name: String,
                             source: String,
                             in_reply_to_status_id: String
                           )

}
