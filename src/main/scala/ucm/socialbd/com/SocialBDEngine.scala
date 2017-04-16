package ucm.socialbd.com

import ucm.socialbd.com.config.SocialBDProperties
import ucm.socialbd.com.jobs.{AirETL, TrafficETL, TwitterETL}

/**
  * Created by Jeff on 15/04/2017.
  */
object SocialBDEngine {
  val etlNames = Set("AIR", "TRAFFIC", "TWITTER")

  def printUsage(exit: Boolean = false): Unit = {
    println ("Arguments:<etl name>")
    println ("etl name must be one of: [" + etlNames.mkString(", ") +"]")
    if (exit)
      sys.exit(1)
  }
  def main(args: Array[String]): Unit = {
    println("-------------------------")
    println("  SocialBigData-CM Engine")
    println("-------------------------")
    if (args.length !=  1 ) printUsage(exit = true)

    val etl = args(0).trim.toUpperCase match {
      case "AIR" => new AirETL(new SocialBDProperties())
      case "TRAFFIC" => new TrafficETL(new SocialBDProperties())
      case "TWITTER" => new TwitterETL(new SocialBDProperties())
      case _ => {
        println (s"Unrecognized etl type ${args(0)}")
        printUsage(exit = false)
        sys.exit(1)
      }
    }
    etl.process()
  }
}
