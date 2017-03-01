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

/**
 * Stream writer that outputs the stream to the standard output.
 */
class LocalPrintWriter extends LocalWriter{

  /**
   * Process the output.
   * <p>
   */
  def output(stream: Seq[String]) = {
    stream.map(rdd => {
      rdd.foreach(x => {println(x)})
    })
  }
}
