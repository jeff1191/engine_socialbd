package ucm.socialbd.com

import org.apache.flink.streaming.api.scala.DataStream
import ucm.socialbd.com.config.SocialBDProperties
import ucm.socialbd.com.process._
import ucm.socialbd.com.sinks.WriterSinkStream

/**
  * Created by Jeff on 15/04/2017.
  */
object SocialBDEngine {
 private val streamNames = Set("AIR", "TRAFFIC", "TWITTER", "BICIMAD", "EMTBUS")

  def printUsage(exit: Boolean = false): Unit = {
    println ("Arguments:<stream type>")
    println ("stream type must be one of: [" + streamNames.mkString(", ") +"]")
    if (exit)
      sys.exit(1)
  }
  def main(args: Array[String]): Unit = {
    println("-------------------------")
    println("  SocialBigData-CM Engine")
    println("-------------------------")
    if (args.length !=  1 ) printUsage(exit = true)

    val streamTransformation = args(0).trim.toUpperCase match {
      case "AIR" => new AirStream(new SocialBDProperties())
      case "TRAFFIC" => new TrafficStream(new SocialBDProperties())
      case "TWITTER" => new TwitterStream(new SocialBDProperties())
      case "BICIMAD" => new BiciMadStream(new SocialBDProperties())
      case "EMTBUS" => new EMTBusStream(new SocialBDProperties())
      case _ => {
        println (s"Unrecognized stream type ${args(0)}")
        printUsage(exit = false)
        sys.exit(1)
      }
    }
    streamTransformation.process
  }
}
