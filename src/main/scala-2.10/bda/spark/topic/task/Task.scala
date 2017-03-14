package bda.spark.topic.task

import com.github.javacliparser.Configurable

/**
  * Created by Roger on 17/3/14.
  */
abstract class Task extends Configurable{

  def run(): Unit
}
