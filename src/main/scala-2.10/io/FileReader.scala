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

package io

import java.io.File

import bda.spark.stream.core.{Example, DocInstance$}
import org.apache.spark.Logging
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

class FileReader(val chunkSize: Int,
                 val slideDuration: Int,
                 val fileName: String
                ) extends StreamReader with Logging {


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
    var line = lines.next()

    new Example(DocInstance.parse(line))
  }

  /**
   * Obtains a stream of examples.
   *
   * @param ssc a Spark Streaming context
   * @return a stream of Examples
   */
  override def getExamples(ssc: StreamingContext): DStream[Example] = {
    new InputDStream[Example](ssc) {
      override def start(): Unit = {}

      override def stop(): Unit = {}

      override def compute(validTime: Time): Option[RDD[Example]] = {
        val examples: Array[Example] = Array.fill[Example](chunkSize)(getExampleFromFile())
        Some(ssc.sparkContext.parallelize(examples))
      }

      override def slideDuration = {
        new Duration(FileReader.this.slideDuration)
      }
    }
  }
}