package bda.spark.topic.task

import bda.spark.topic.stream.OnlineLDA
import bda.spark.topic.stream.io.{FileReader, PrintStreamWriter, StreamReader, StreamWriter}
import org.apache.spark.streaming.StreamingContext

/**
  * Created by Roger on 17/2/22.
  */
class StreamOnlineLDATask {

  def run(ssc: StreamingContext): Unit = {

    val reader: StreamReader = new FileReader(1000, 1000, "/home/gty/data/economy_doc_2016_merge")
    val writer: StreamWriter = new PrintStreamWriter()
    val instances = reader.getExamples(ssc)

    val learner = new OnlineLDA(10, 1, 1)
    //Train
    learner.train(instances)
    /*val predicts: DStream[String] = learner.assign(instances).map{
      example =>
        example.instance.tokens.map{
          case (term, topic) =>
            (topic, 1)
        }.groupBy(_._1).map{
          entry=>
            val topic = entry._1
            val cnt = entry._2.size
            (topic, cnt)
        }.toSeq.sortBy( - _._2).map{
          case (topic, cnt) =>
            s"$topic($cnt)"
        }.mkString(" ")
    }
    writer.output(predicts)
    */
  }
}
