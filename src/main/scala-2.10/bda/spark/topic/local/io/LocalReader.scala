package bda.spark.topic.local.io

import bda.spark.topic.core.Example

/**
  * Created by Roger on 17/2/24.
  */
abstract class LocalReader {

  def getExamples(): Seq[Example]
}
