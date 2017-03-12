package bda.spark.topic.task

import java.io.PrintWriter

import bda.spark.topic.core.{TextDocInstance$, Example}
import bda.spark.topic.local.io.StopWordRecognition
import org.ansj.splitWord.analysis.ToAnalysis

import scala.io.Source
import scala.util.parsing.json.JSON
import scala.collection.JavaConversions._

/**
  * Created by Roger on 17/2/24.
  */
object localFileSplit {

  def getExample(line: String) = {
    val json = JSON.parseFull(line).get.asInstanceOf[Map[String, Any]]
    val content = json("c").toString
    val id = json("id")
    val terms = ToAnalysis.parse(content).
      recognition(StopWordRecognition.getStopRecognition()).getTerms.toIterator
    val segLine = terms.map(_.getName).mkString(" ")
    (id, segLine)
  }
  def main(args:Array[String]): Unit ={
    val fileName = "/home/gty/data/economy_doc_2016_merge"
    val output = "/home/gty/data/economy_sent_docs_2016"
    val  lines = Source.fromFile(fileName).getLines()

    val writer = new PrintWriter(s"$output")
    while( lines.hasNext ){
      var line = lines.next().replace('\n', ' ').replace('\r', ' ')
      val (id, example) = getExample(line)
      writer.println(example)
    }
    writer.close()
   }
}
