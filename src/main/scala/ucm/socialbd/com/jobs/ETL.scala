package ucm.socialbd.com.jobs

import org.apache.flink.streaming.api.scala.DataStream
import ucm.socialbd.com.dataypes.EnrichmentObj

/**
  * Created by Jeff on 16/04/2017.
  */
trait ETL extends Serializable{
  def process(): Unit
  def sendToElastic(enrDataStream : DataStream[EnrichmentObj]): Unit
}
