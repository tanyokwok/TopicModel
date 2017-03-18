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

import java.util

import bda.spark.topic.glint.Glint
import bda.spark.topic.redis.{RedisLock, RedisVocab, RedisVocabClient, RedisVocabPipeline}
import breeze.linalg.{DenseMatrix, DenseVector}
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable
import glint._
import glint.models.client.{BigMatrix, BigVector}
import org.apache.spark.Logging
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster}

import collection.JavaConversions._
import scala.collection.immutable.HashSet
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Latent Dirichlet Allocation (LDA) model.
  *
  * This abstraction permits for different underlying representations,
  * including local and distributed data structures.
  */
abstract class LdaModel() extends Serializable{
  def K: Int

  def alpha: Int

  def beta: Int

  def vocabulary: Set[String]

  type TOPICASSIGN = (String,Int)
  type QUEUE = mutable.PriorityQueue[TOPICASSIGN]

  def topicWords(T: Int): Seq[QUEUE]

  def estimatedMemUse() = {
    if (vocabulary == null) {
      -1
    } else {
      val vocab_bytes = vocabulary.map(_.getBytes.length).sum
      (vocabulary.size) * K * 8 + vocab_bytes
    }
  }
}

/*
class OnlineLDAModel(override val k:Int,
                     override val alpha: Double,
                     override val beta: Double) extends LDAModel{

}*/

/**
  * @param K     Number of topics
  * @param alpha alpha for the prior placed on documents'
  *              distributions over topics ("theta").
  *              This is the parameter to a Dirichlet distribution.
  * @param beta beta for the prior placed on topics' distributions over terms.
  */
class OnlineLdaModel(val iteration: Int,
                     override val K: Int,
                     override val alpha: Int,
                     override val beta:Int,
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
                     override val alpha:Int,
                     override val beta: Int,
                     val betaStatsMatrix: DenseMatrix[Int],
                     val word2id: Map[String, Int]
                    ) extends LdaModel with Serializable{


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
                 override val alpha:Int,
                 override val beta: Int,
                 val redisVocab: RedisVocabClient
                ) extends LdaModel with Logging{
  def V = redisVocab.vocabSize
  @transient
  val client = glint.Client(ConfigFactory.parseFile(new java.io.File("resources/glint.conf")))
  val betaStatsMatrix: BigMatrix[Double] = client.matrix[Double](V, K)
  val topicStatsVector: BigVector[Double] = client.vector[Double](K)

  override def vocabulary: Set[String] = {
    redisVocab.fetchLock("PsLdaModel")
    val ret = redisVocab.loadVocab.values
    redisVocab.relaseLock("PsLdaModel")
    ret.toSet
  }

  override def topicWords(T: Int): Seq[QUEUE] = ???
}

class PsStreamLdaModel(val K: Int,
                       val V: Long,
                       val alpha: Double,
                       val beta: Double,
                       val rate: Double,
                       val host: String,
                       val port: Int,
                       val expired: Long,
                      val duration: Long
                      ) extends Serializable with Logging{


  private val lockKey = "lda.ps.lock"
  private val jedisClusterNodes = HashSet[HostAndPort](new HostAndPort(host, port))

  @transient
  lazy val jedis = {
    new Jedis(host, port)
    //new JedisCluster(jedisClusterNodes)
  }

  jedis.del(lockKey)
  @transient
  lazy val lock = {
    //val jedis: JedisCluster = new JedisCluster(jedisClusterNodes)
    val ret = new RedisLock(jedis, lockKey, expired)
    ret
  }

  @transient
  lazy val redisVocab = new RedisVocab(V, jedis, expired)

  redisVocab.clear()

  @transient
  val client = glint.Client(ConfigFactory.parseFile(new java.io.File("resources/glint.conf")))
  val priorWordTopicCountMat: BigMatrix[Double] = client.matrix[Double](V, K)
  val priorTopicCountVec: BigVector[Double] = client.vector[Double](K)

  def destroy(): Unit ={
    priorTopicCountVec.destroy()
    priorWordTopicCountMat.destroy()
    client.stop()
    redisVocab.clear()
    lock.clear()
  }

  type TOPICASSIGN = (String, Double)
  type QUEUE = mutable.PriorityQueue[TOPICASSIGN]

  def topicWords(T:Int) = {
    val queues = Array.fill[QUEUE](K)(new QUEUE()(Ordering.by(-_._2)))

    val lockToken = "PsStreamLdaModel"
    redisVocab.fetchLock(lockToken)
    val id2word = redisVocab.loadVocab

    val ids = id2word.map(_._1).toArray
    val matrix = Glint.pullData(ids, priorWordTopicCountMat)
    val vector = Glint.pullData((0L until K.toLong).toArray, priorTopicCountVec)
    val (batchTime, lastUpdateTime) = redisVocab.getLastUpdateBatchTime(ids)

    var totCount = 0.0
    ids.zip(lastUpdateTime).foreach{
      case (id, lastTime) =>
        assert(lastTime <= batchTime)
        if (lastTime < batchTime) {
          val batchDelta = (batchTime - lastTime) / duration
          for (i <- 0 until K) {
            matrix(id)(i) = matrix(id)(i)*math.pow(1 - rate, batchDelta)
          }
        }
        totCount += matrix(id).sum
    }


    matrix.foreach{
      case (wid, vec) =>
        Range(0, K).foreach{
          k =>
            queues(k).enqueue((id2word(wid), vec(k)))
            if (T < queues(k).size) {
              queues(k).dequeue()
            }
        }
    }

    println(s"count: ($totCount, ${vector.sum})")


    redisVocab.relaseLock(lockToken)
    queues
  }

  def check(token: String): Unit = {
    val id2word = redisVocab.loadVocab
    val ids = id2word.map(_._1).toArray
    println("vocabSize:" + ids.size + " " + ids.distinct.size)

    val matrix = Glint.pullData(ids, priorWordTopicCountMat)
    val vector = Glint.pullData((0L until K.toLong).toArray, priorTopicCountVec)
    val (batchTime, lastUpdateTime) = redisVocab.getLastUpdateBatchTime(ids)

    var totCount = 0.0
    ids.zip(lastUpdateTime).foreach {
      case (id, lastTime) =>
        assert(lastTime <= batchTime)
        if (lastTime < batchTime) {
          val batchDelta = (batchTime - lastTime) / duration
          for (i <- 0 until K) {
            matrix(id)(i) = matrix(id)(i) * math.pow(1 - rate, batchDelta)
          }
        }
        totCount += matrix(id).sum
    }

    logInfo(s"[$token] count: ($totCount, ${vector.sum})")
  }

  def addParameter(rows: Array[Long], time: Long): Unit = {
    val deltaVec = Array.fill[Double](K)(rows.size * beta)
    val deltaMat = rows.flatMap{
      row =>
        Range(0, K).map{
          k =>
            (row, k, beta)
        }
    }
    Glint.pushData(deltaMat, priorWordTopicCountMat)
    Glint.pushData(deltaVec, priorTopicCountVec)
    redisVocab.updateBatchTime(rows, time)
  }

  def lazyClearParameter(rows: Array[Long], time: Long) = {
    val (batchTime, lastUpdateTimes) = redisVocab.getLastUpdateBatchTime(rows)
    val matrix = Glint.pullData(rows, priorWordTopicCountMat)

    val deltaVec = Array.fill[Double](K)(0)
    if (batchTime < time) {
      val batchDelta = if (batchTime >= 0) (time - batchTime) / duration else 1
      val yeta = math.pow(1 - rate , batchDelta)
      val nt_ = Glint.pullData((0L until K.toLong).toArray, priorTopicCountVec)
      for (i <- 0 until K) {
        deltaVec(i) += (yeta - 1) * nt_(i)
      }
    }
    val deltaMatrix = rows.zip(lastUpdateTimes).filter{
      case (wid, lastTime) =>
        lastTime < time
    }.map{
      case (wid, lastTime) =>
        val batchDelta = if (lastTime >= 0) (time - lastTime) / duration else 1
        val yeta = math.pow(1 - rate , batchDelta)
        val nwt_ = matrix(wid)
        Range(0, K).map{
          i =>
            deltaVec(i) += (beta - nwt_(i) * yeta)
            (wid, i, beta - nwt_(i))
        }
    }.flatMap(_.toSeq)

    println("new word: " + lastUpdateTimes.filter( _ < 0 ).size)

    Glint.pushData(deltaMatrix, priorWordTopicCountMat)

    val nt_ = Glint.pullData((0L until K.toLong).toArray, priorTopicCountVec)
    println("nt_ :" + nt_.mkString(" "))
    Glint.pushData(deltaVec, priorTopicCountVec)
    println("deltaVec:" + deltaVec.mkString(" "))
    val nt_2 = Glint.pullData((0L until K.toLong).toArray, priorTopicCountVec)
    println("nt_2 :" + nt_2.mkString(" "))
    redisVocab.updateBatchTime(rows, time)
    check("lazyClear")
  }


  /**
    * 将词Id对应的参数更新为最新的状态
    * @param wordIds
    * @param batchTime 当前时间
    * @return
    */
  def lazySyncParameter(wordIds: Array[Long], batchTime: Long):
  (Array[Double], Map[Long, Array[Double]]) = {
    val (lastBatchTime , lastUpdateTimes) = redisVocab.getLastUpdateBatchTime(wordIds)
    val priorNwt = Glint.pullData(wordIds, priorWordTopicCountMat)
    val priorNt = Glint.pullData((0L until K.toLong).toArray, priorTopicCountVec)

    val deltaVec = Array.fill[Double](K)(0)
    if (lastBatchTime < batchTime) {
      val batchDelta = if (lastBatchTime >= 0) (batchTime - lastBatchTime) / duration else 1
      logInfo(s"batchDelta: $batchTime - $lastBatchTime / $duration = " + batchDelta)
      val yeta = math.pow(1 - rate , batchDelta)
      // nt = math.pow(rate, batchDelta) * nt
      for (i <- 0 until K) {
        deltaVec(i) += (yeta - 1) * priorNt(i)
      }
      Range(0, priorNt.length).foreach(k=> priorNt(k) *= yeta)
    }

    assert(lastUpdateTimes.filter(x => x < 0).size == 0)
    var minusCount = 0.0
    val deltaNwt = wordIds.zip(lastUpdateTimes).filter{
      case (wid, lastTime) =>
        lastTime < batchTime
    }.map{
      case (wid, lastTime) =>
        val batchDelta = if(lastTime >= 0) (batchTime - lastTime) / duration else 1
        val yeta = math.pow(1 - rate , batchDelta)
        val nwt: Array[Double] = priorNwt(wid)
        Range(0 , K).map {
          i =>
            val ret = nwt(i) * (yeta - 1)
            minusCount += ret
            nwt(i) *= yeta
            (wid, i, ret)
        }
    }.flatMap(_.toSeq)
    check("lazySync")
    Glint.pushData(deltaNwt, priorWordTopicCountMat)
    //Glint.pushData(deltaVec, priorTopicCountVec)
    redisVocab.updateBatchTime(wordIds, batchTime)
    logInfo("minusCount: " + minusCount)
    check("lazySync")
    (priorNt, priorNwt)
  }


  def update(wordIds: Array[Long],
                     wordTopicCounts: Seq[(Long, Int, Double)],
                     time: Long) = {

    val wordTopicDelta = wordTopicCounts.map{
      case (wid, topic, cnt) =>
        (wid, topic, cnt * rate)
    }

    Glint.pushData(wordTopicDelta, priorWordTopicCountMat)

    val topicDelta = Array.fill[Double](K)(0)
    wordTopicDelta.foreach {
      case (wid, topic, cnt) =>
        topicDelta(topic) += cnt
    }
    Await.result(
      priorTopicCountVec.push((0L until K.toLong).toArray, topicDelta),
      Duration.Inf
    )

    logInfo("topicDelta: " + topicDelta.mkString(" "))
    val rest = Glint.pullData((0L until K.toLong).toArray, priorTopicCountVec)
    logInfo("after update: " + rest.mkString(" "))

    check("update")
  }

}

