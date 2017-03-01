package bda.spark.topic.stream.io

import org.apache.spark.streaming.dstream.DStream

/**
  * Created by Roger on 17/2/23.
  */
class FileWriter(fileName: String) extends StreamWriter with Serializable {
  /**
    * Process the output.
    *
    * @param stream a DStream of Strings for which the output is processed
    */
  override def output(stream: DStream[String]): Unit = {
    stream.saveAsTextFiles(fileName)
  }
}
