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

package bda.spark.topic.stream.io

import bda.spark.topic.core.{Example, Instance, TextDocInstance}
import bda.spark.topic.stream.preprocess.Formatter
import com.github.javacliparser.{ClassOption, Configurable, IntOption, StringOption}
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Duration, StreamingContext, Time}

import scala.io.Source

/**
 * FileReader is used to read data from one file of full data to simulate a stream data.
 *
 * <p>It uses the following options:
 * <ul>
 *  <li> Chunk size (<b>-k</b>)
 *  <li> Slide duration in milliseconds (<b>-d</b>)
 *  <li> Data File Name (<b>-f</b>)
 * </ul>
 */

class FileReader extends StreamReader {


  val chunkSizeOption: IntOption = new IntOption("chunkSize", 'k',
    "Chunk Size", 10000, 1, Integer.MAX_VALUE)

  val slideDurationOption: IntOption = new IntOption("slideDuration", 'd',
    "Slide Duration in milliseconds", 60000, 1, Integer.MAX_VALUE)

  val fileNameOption: StringOption = new StringOption("fileName", 'f',
    "File Name", "data/economy_sent_docs_2016_mini")

  val formatterOption: ClassOption = new ClassOption("formatter", 't',
    "the formatter for input lines",
    classOf[Formatter], "Formatter")

  val chunkSize = chunkSizeOption.getValue
  val slideDuration = slideDurationOption.getValue
  val fileName = fileNameOption.getValue
  val formatter = formatterOption.getValue[Formatter]

  var lines: Iterator[String] = null
  /**
   * Get one Exmaple from file
   *
   * @return an Exmaple
   */
  def getInstanceFromFile(): Instance= {
    if (lines == null || !lines.hasNext) {
      lines = Source.fromFile(fileName).getLines()
    }
    // if reach the end of file, will go to the head again
    if (!lines.hasNext) {
      lines = Source.fromFile(fileName).getLines()
    }

    formatter.format(lines.next())
  }

  /**
   * Obtains a stream of examples.
   *
   * @param ssc a Spark Streaming context
   * @return a stream of Examples
   */
  override def getInstances(ssc: StreamingContext): DStream[Instance] = {
    new InputDStream[Instance](ssc) {
      override def start(): Unit = {}

      override def stop(): Unit = {}

      override def compute(validTime: Time): Option[RDD[Instance]] = {
        val examples: Array[Instance] = Array.fill[Instance](chunkSize)(getInstanceFromFile())
        Some(ssc.sparkContext.parallelize(examples))
      }

      override def slideDuration = {
        new Duration(FileReader.this.slideDuration)
      }
    }
  }
}
