package bda.spark.topic.core

/**
  * Created by Roger on 17/2/22.
  */

//每个token均为(term, topic)的元组
class DocInstance(val tokens: DOC) extends Serializable{
  override def toString: String = {
    tokens.map {
      case (term, topic) =>
        s"$term($topic) "
    }.reduce {
      (x, y) =>
        s"$x $y"
    }
  }
}

object DocInstance {

  def parse(input:String):DocInstance = {
    val tokens = input.split("\\s+")
    new DocInstance(tokens.map((_,0)))
  }
}
