package ucm.socialbd.com.jobs

/**
  * Created by Jeff on 16/04/2017.
  */
trait ETL extends Serializable{
  def process(): Unit
}
