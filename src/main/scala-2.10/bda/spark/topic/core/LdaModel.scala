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

package bda.spark.topic.core

import breeze.linalg.{DenseMatrix, DenseVector}

import scala.collection.mutable
import glint._
import glint.models.client.BigMatrix;

/**
  * Latent Dirichlet Allocation (LDA) model.
  *
  * This abstraction permits for different underlying representations,
  * including local and distributed data structures.
  */
abstract class LdaModel() extends Serializable{
  def K: Int

  def ALPHA: Int

  def BETA: Int

  def vocabulary: Set[String]

  type TOPICASSIGN = (String,Int)
  type QUEUE = mutable.PriorityQueue[TOPICASSIGN]

  def topicWords(T: Int): Seq[QUEUE]

  def estimatedMemUse() = {
    val vocab_bytes = vocabulary.map(_.getBytes.length).sum
    (vocabulary.size)* K * 8 + vocab_bytes
  }
}

/*
class OnlineLDAModel(override val k:Int,
                     override val alpha: Double,
                     override val beta: Double) extends LDAModel{

}*/

/**
  * @param K     Number of topics
  * @param ALPHA alpha for the prior placed on documents'
  *              distributions over topics ("theta").
  *              This is the parameter to a Dirichlet distribution.
  * @param BETA beta for the prior placed on topics' distributions over terms.
  */
class OnlineLdaModel(val iteration: Int,
                     override val K: Int,
                     override val ALPHA: Int,
                     override val BETA:Int,
                     val prioriAlphaStatsVector: Array[Int],
                     val prioriBetaStatsMatrix: Map[String, Array[Int]]
                    ) extends LdaModel with Serializable{


  override def vocabulary = prioriBetaStatsMatrix.keySet

  val prioriBetaStatsVector: Array[Int] = {
    prioriBetaStatsMatrix.values.fold(Array.fill[Int](K)(0)){
      (ent1, ent2) =>
        val z: Array[Int] = ent1.zip( ent2).map{
          case (x, y) =>
            x + y
        }
        z
    }
  }


  override def topicWords(T:Int) = {
    val queues = Array.fill[QUEUE](K)(new QUEUE()(Ordering.by(-_._2)))
    prioriBetaStatsMatrix.foreach{
      case (term, topics) =>
        Range(0, K).foreach{
          k =>
            queues(k).enqueue((term, topics(k)))
            if (T < queues(k).size) {
              queues(k).dequeue()
            }
        }
    }

    queues
  }

}


class SimpleLdaModel(override val K:Int,
                     override val ALPHA:Int,
                     override val BETA: Int,
                     val betaStatsMatrix: DenseMatrix[Int],
                     val word2id: Map[String, Int]
                    ) extends LdaModel{


  override def topicWords(T:Int) = {
    val queues = Array.fill[QUEUE](K)(new QUEUE()(Ordering.by(-_._2)))

    word2id.foreach{
      case (term, id) =>
        Range(0, K).foreach{
          k =>
            queues(k).enqueue((term, betaStatsMatrix(id,k)))
            if (T < queues(k).size) {
              queues(k).dequeue()
            }
        }
    }

    queues
  }

  override def vocabulary = word2id.keySet
}

class PsLdaModel(override val K:Int,
                 override val ALPHA:Int,
                 override val BETA: Int,
                 val betaStatsMatrix: BigMatrix[Double],
                 val word2id: Map[String, Int]) extends LdaModel{
  override def vocabulary: Set[String] = word2id.keySet

  override def topicWords(T: Int): Seq[QUEUE] = ???
}

