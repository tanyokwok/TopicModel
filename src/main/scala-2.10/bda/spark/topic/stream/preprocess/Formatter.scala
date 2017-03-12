package bda.spark.topic.stream.preprocess

import bda.spark.topic.core.TextDocInstance
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis

import scala.util.parsing.json.JSON
import collection.JavaConversions._
import scala.io.Source

/**
  * Created by Roger on 17/3/10.
  */
class Formatter {
  def format(line:String): TextDocInstance = {
    TextDocInstance.parse(line)
  }
}

class JsonSegFormatter extends Formatter{

  val recognition = {
    val recognition = new StopRecognition
    val stops = Source.fromFile("resources/stopword").getLines()

    recognition.insertStopWords(stops.toSeq)
    recognition.insertStopNatures("null")
    recognition
  }

  override def format(line:String): TextDocInstance = {

    val nline = line.replace('\n', ' ').replace('\r', ' ')

    val content = JSON.parseFull(nline).
      get.asInstanceOf[Map[String, Any]]("c").toString

    val terms= ToAnalysis.parse(content).
      recognition(recognition).getTerms.toIterator
    val segLine = terms.map(_.getName).mkString(" ")

    super.format(segLine)
  }

}