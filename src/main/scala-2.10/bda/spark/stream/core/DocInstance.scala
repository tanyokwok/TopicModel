package bda.spark.stream.core

/**
  * Created by Roger on 17/2/22.
  */

//每个token均为(term, topic)的元组
class DocInstance(val tokens: DOC){
}

object DocInstance {

  def parse(input:String):DocInstance = {
    val tokens = input.split("\\s+")
    new DocInstance(tokens.map((_,0)))
  }
}
