/*
 * Copyright (C) 2015 Holmes Team at HUAWEI Noah's Ark Lab.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package bda.spark.topic.local.io

import bda.spark.topic.core.{DocInstance, Example}
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.Logging

import scala.collection.JavaConversions._
import scala.io.Source
import scala.util.parsing.json.JSON

object StopWordRecognition {

  def getStopRecognition(): StopRecognition = {
    val recognition = new StopRecognition
    val stops = Source.fromFile("resources/stopword").getLines()

    recognition.insertStopWords(stops.toSeq)
    recognition.insertStopNatures("null")
    recognition
  }
}

/**
  * FileReader is used to read data from one file of full data to simulate a stream data.
  *
  * <p>It uses the following options:
  * <ul>
  * <li> Chunk size (<b>-k</b>)
  * <li> Slide duration in milliseconds (<b>-d</b>)
  * <li> Data File Name (<b>-f</b>)
  * </ul>
  */

class AutoSegFileReader(val chunkSize: Int,
                        val fileName: String
                ) extends LocalReader with Logging {


  val recognition = StopWordRecognition.getStopRecognition()
  var lines: Iterator[String] = null

  /**
    * Get one Exmaple from file
    *
    * @return an Exmaple
    */
  def getExampleFromFile(): Example = {
    if (lines == null || !lines.hasNext) {
      lines = Source.fromFile(fileName).getLines()
    }
    // if reach the end of file, will go to the head again
    if (!lines.hasNext) {
      lines = Source.fromFile(fileName).getLines()
    }
    var line = lines.next().replace('\n', ' ').replace('\r', ' ')
    parse(line)
  }

  private def parse(line:String):Example ={
    val content = JSON.parseFull(line).get.asInstanceOf[Map[String, Any]]("c").toString
    val terms = ToAnalysis.parse(content).
      recognition(recognition).getTerms.toIterator
    val segLine = terms.map(_.getName).mkString(" ")
    new Example(DocInstance.parse(segLine))
  }


  /**
    * Obtains a stream of examples.
    *
    * @return a stream of Examples
    */
  override def getExamples(): Seq[Example] = {

    val examples: Array[Example] = Array.fill[Example](chunkSize)(getExampleFromFile())
    examples
  }

  def getAllExamples():Seq[Example] = {

    lines = Source.fromFile(fileName).getLines()
    lines.map{
      line =>
      parse(line.replace('\n', ' ').replace('\r', ' '))
    }.toSeq
  }
}

object TestFileReader {

  def main(args: Array[String]): Unit = {
    val reader = new AutoSegFileReader(10000, "data/test_data")
    var examples = Array.fill[Example](9)(reader.getExampleFromFile())

    examples.zipWithIndex.foreach {
      case (line, index) =>
        println(s"$index $line")
    }

    examples = Array.fill[Example](9)(reader.getExampleFromFile())
    examples.zipWithIndex.foreach {
      case (line, index) =>
        println(s"$index $line")
    }

  }
}