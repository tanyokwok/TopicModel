package bda.spark.topic.core

class Instance extends Serializable{
}
/**
  * Created by Roger on 17/2/22.
  */

//每个token均为(term, topic)的元组
class TextDocInstance(val tokens: TextDoc) extends Instance with Serializable{
  override def toString: String = {
    if (tokens.size == 0) {
      return ""
    }

    tokens.map {
      case (term, topic) =>
        s"$term($topic) "
    }.reduce {
      (x, y) =>
        s"$x $y"
    }
  }
}

class IdDocInstance(val tokens: IdDoc) extends Instance with Serializable{
 override def toString: String = {
   if (tokens.size == 0) ""
   else {
     tokens.map {
       case (term, topic) =>
         s"$term($topic) "
     }.reduce {
       (x, y) =>
         s"$x $y"
     }
   }
  }
}

object TextDocInstance {

  def parse(input:String):TextDocInstance = {
    val tokens = input.split("\\s+")
    new TextDocInstance(tokens.map((_,0)))
  }
}
