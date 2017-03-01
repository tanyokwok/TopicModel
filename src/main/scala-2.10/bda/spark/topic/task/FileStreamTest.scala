package bda.spark.topic.task

import bda.spark.topic.local.io.SimpleFileReader

import scala.io.Source

/**
  * Created by Roger on 17/2/26.
  */
object FileStreamTest {

  def main(args: Array[String]): Unit ={

    val train_file = "data/economy_sent_docs_2016_mini"
    /*val reader: SimpleFileReader = new SimpleFileReader(1000, train_file)

    val instances = reader.getAllExamples()*/

    val lines = Source.fromFile(train_file).getLines()

    //lines.take(10).foreach(println)
    val b = lines.map{
      example =>
        println(example)
        example
    }
    b.foreach(ele => ele)
  }

}
