package bda.spark.topic.core

/**
  * Created by Roger on 17/2/22.
  */
class Example(val instance: DocInstance) extends Serializable{

  override def toString: String = instance.toString
}
