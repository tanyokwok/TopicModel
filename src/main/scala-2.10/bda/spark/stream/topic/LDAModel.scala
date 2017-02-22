/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bda.spark.stream.topic

/**
  * Latent Dirichlet Allocation (LDA) model.
  *
  * This abstraction permits for different underlying representations,
  * including local and distributed data structures.
  */
abstract class LDAModel() {
  def K: Int

  def ALPHA: Int

  def BETA: Int
}

/*
class OnlineLDAModel(override val k:Int,
                     override val alpha: Double,
                     override val beta: Double) extends LDAModel{

}*/

/**
  * @param k     Number of topics
  * @param alpha alpha for the prior placed on documents'
  *              distributions over topics ("theta").
  *              This is the parameter to a Dirichlet distribution.
  * @param beta  beta for the prior placed on topics' distributions over terms.
  */
class OnlineLDAModel(k: Int,
                     alpha: Int,
                     beta: Int,
                     val prioriAlphaStatsVector: Array[Int],
                     val prioriBetaStatsMatrix: scala.collection.mutable.Map[String, Array[Int]]
                    ) extends LDAModel with Serializable{



  def addNewTerm(term: String): Unit = {
    if (vocabulary.contains(term)) return
    else {
      prioriBetaStatsMatrix += ((term, Array.fill[Int](K)(beta)))
    }
  }

  override val K = k

  override val ALPHA = alpha

  override val BETA = beta

  def vocabulary = prioriBetaStatsMatrix.keySet

  def prioriBetaStatsVector(): Array[Int] = {
    prioriBetaStatsMatrix.values.fold(Array.fill[Int](K)(0)){
      (ent1, ent2) =>
        ent1.zip( ent2).map{
          case (x, y) =>
            x + y
        }
    }
  }
}



