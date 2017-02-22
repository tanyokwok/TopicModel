package bda.spark.stream.task

import bda.spark.stream.topic.OnlineLDA
import io.{FileReader, StreamReader}
import org.apache.spark.streaming.StreamingContext

/**
  * Created by Roger on 17/2/22.
  */
class OnlineLDATask {

  def run(ssc: StreamingContext): Unit = {

    val reader: StreamReader = new FileReader(10000, 1000, "")
    val instances = reader.getExamples(ssc)

    val learner = new OnlineLDA(10, 1, 1)
    //Train
    learner.train(instances)

  }
}
