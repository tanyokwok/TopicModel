package bda.spark.topic.task

import bda.spark.topic.local.OnlineLDA
import bda.spark.topic.local.io._
import org.apache.spark.streaming.StreamingContext

/**
  * Created by Roger on 17/2/22.
  */
class LocalOnlineLDATask {

  def run(): Unit = {

    val reader: LocalReader= new AutoSegFileReader(1000, "/home/gty/data/economy_doc_2016_merge")
    val writer: LocalWriter = new LocalPrintWriter()
    val learner = new OnlineLDA(10, 1,1 )

    val T = 10000
    Range(0, T).foreach{
      t =>
        val instances = reader.getExamples()
        learner.train(instances)
    }
    //Train
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

