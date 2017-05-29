package ucm.socialbd.com.process

import org.apache.flink.streaming.api.scala.DataStream
import ucm.socialbd.com.dataypes.EnrichmentObj

/**
  * Created by Jeff on 16/04/2017.
  */
trait StreamTransform extends Serializable{
  def process(): Unit
  def writeDataStreamToSinks(enrDataStream: DataStream[String]): Unit
}
