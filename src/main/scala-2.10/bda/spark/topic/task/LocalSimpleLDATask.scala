package bda.spark.topic.task

import bda.spark.topic.core.Example
import bda.spark.topic.local.{OnlineLDA, SimpleLDA}
import bda.spark.topic.local.io._
import bda.spark.topic.utils.Timer

/**
  * Created by Roger on 17/2/22.
  */
class LocalSimpleLDATask {

  def run(train_file:String, test_file:String, iteration: Int): Unit = {

    val reader: SimpleFileReader = new SimpleFileReader(1000, train_file)
    val testReader: SimpleFileReader = new SimpleFileReader(1000, test_file)
    val writer: LocalWriter = new LocalPrintWriter()
    val learner = new SimpleLDA(10, 1,1 )

    val timer = new Timer()
    val instances: Seq[Example] = reader.getAllExamples()
    val testSet: Seq[Example] = testReader.getAllExamples()
    //instances.foreach( println )
    println(s"Using ${timer.getReadableRunnningTime()} to read all examples")
    learner.train(instances, testSet, iteration)
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

object LocalSimpleLDATask{

  def main(args: Array[String]): Unit ={
    val task = new LocalSimpleLDATask()
    //run task
    if (args.length > 2) {
      task.run(args(0), args(1), args(2).toInt)
    }
    else {
      //task.run("/home/gty/data/economy_sent_docs_2016_mini")
      task.run("data/economy_sent_docs_2016_mini", "data/economy_sent_docs_2016_test", 100)
      //task.run("data/train_data")
    }
  }
}