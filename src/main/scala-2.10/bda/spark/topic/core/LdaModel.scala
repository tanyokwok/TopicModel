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
import bda.spark.topic.redis._
import breeze.linalg.{DenseMatrix, DenseVector}
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable
import glint._
import glint.models.client.{BigMatrix, BigVector}
import org.apache.spark.Logging
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster, JedisPool}

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
  private val stageKey = "lda.stage.status"
  val STAGE_FINISH = "STAGE_FINISH"
  val STAGE_RUNING = "STAGE_RUNING"
  val STAGE_BUILD_VOCAB = "STAGE_BUILD_VOCAB"

  @transient
  lazy val jedisPool = {
    new JedisPool(host, port)
    //new JedisCluster(jedisClusterNodes)
  }

  @transient
  lazy val lock = {
    val ret = new RedisPoolLock(jedisPool, lockKey, expired)
    ret
  }

  def setStage(time: Long, status: String): Boolean= {
    var jedis: Jedis = null

    try{
      jedis = jedisPool.getResource
      val stage = jedis.get(stageKey)
      if (stage == null) {
        println(s"Set `$time $status`, current is `$stage`")
        jedis.setex(stageKey, expired.toInt, s"$time $status")
      } else {
        var flag = true
        while (flag) {
          val arr = stage.split(" ")
          val stageTime = arr(0).toLong
          val stageStatus = arr(1)

          if (stageTime > time) {
            return false
          } else if (stageTime < time) {
            if (stageStatus == STAGE_FINISH) {
              println(s"Set `$time $status`, current is `$stage`")
              jedis.setex(stageKey, expired.toInt, s"$time $status")
              flag = false
            }
          } else {
            println(s"Set `$time $status`, current is `$stage`")
            jedis.setex(stageKey, expired.toInt, s"$time $status")
            flag = false
          }

          if (flag) {
            Thread.sleep(1000)
          }
        }
      }

      return true
    } finally{
      if (jedis != null) {
        jedis.close()
      }
    }
  }

  @transient
  lazy val redisVocab ={
    //val jedis1 = new Jedis(host, port)
    //val jedis2 = new Jedis(host, port)

    val lock = new RedisPoolLock(jedisPool, "lda.vocab.lock", expired)
    new RedisPoolVocab(V, jedisPool, lock, expired)
  }

  def clear(): Unit = {
    var jedis: Jedis = null
    try{
      jedis = jedisPool.getResource
      jedis.del(stageKey)
      jedis.del(lockKey)
    } finally{
      if (jedis != null) jedis.close()
    }
    redisVocab.clear()
  }

  clear()


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
    jedisPool.close()
  }

  type TOPICASSIGN = (String, Double)
  type QUEUE = mutable.PriorityQueue[TOPICASSIGN]

  def topicWords(T:Int) = {
    val queues = Array.fill[QUEUE](K)(new QUEUE()(Ordering.by(-_._2)))

    val lockToken = "PsStreamLdaModel"
    redisVocab.fetchLock(lockToken)
    val id2word = redisVocab.loadVocab
    val ids = id2word.map(_._1).toArray
    val (batchTime, lastUpdateTime) = redisVocab.getLastUpdateBatchTime(ids)
    val idTimes = ids.zip(lastUpdateTime).filter( _._2 <= batchTime)
    val matrix = Glint.pullData(idTimes.map(_._1), priorWordTopicCountMat)
    val vector = Glint.pullData((0L until K.toLong).toArray, priorTopicCountVec)
    redisVocab.relaseLock(lockToken)

    var totCount = 0.0
    idTimes.foreach{
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
    queues
  }

  def check(token: String): Unit = {
    val id2word = redisVocab.loadVocab
    val ids = id2word.map(_._1).toArray

    val (batchTime, lastUpdateTime) = redisVocab.getLastUpdateBatchTime(ids)
    val idTimes = ids.zip(lastUpdateTime).filter( _._2 <= batchTime)
    val matrix = Glint.pullData(idTimes.map(_._1), priorWordTopicCountMat)
    val vector = Glint.pullData((0L until K.toLong).toArray, priorTopicCountVec)

    var totCount = 0.0
    var totSum = 0.0
    idTimes.foreach{
      case (id, lastTime) =>
        totSum += matrix(id).sum
        if (lastTime < batchTime) {
          val batchDelta = (batchTime - lastTime) / duration
          for (i <- 0 until K) {
            matrix(id)(i) = matrix(id)(i) * math.pow(1 - rate, batchDelta)
          }
        }
        totCount += matrix(id).sum
    }

    logInfo(s"[$token] count: ($totSum, $totCount, ${vector.sum})")
  }


  /**
    * 当词被替换的时候为了更新PS时调用
    * @param batchTime
    */
  def lazyClearWordParameter(wordIdTimes: Array[(Long, Long)], lastBatchTime: Long, batchTime: Long, token: String) = {
    lock.fetchLock(token + "-lazyClear")
    val rows = wordIdTimes.map(_._1)
    val matrix = Glint.pullData(rows, priorWordTopicCountMat)

    val deltaVec = Array.fill[Double](K)(0)
    if (lastBatchTime < batchTime) {
      val batchDelta = if (lastBatchTime >= 0) (batchTime - lastBatchTime) / duration else 1
      val yeta = math.pow(1 - rate , batchDelta)
      val nt_ = Glint.pullData((0L until K.toLong).toArray, priorTopicCountVec)
      for (i <- 0 until K) {
        deltaVec(i) += (yeta - 1) * nt_(i)
      }
    }
    val deltaMatrix = wordIdTimes.filter{
      case (wid, lastTime) =>
        lastTime < batchTime
    }.map{
      case (wid, lastTime) =>
        val batchDelta = if (lastTime >= 0) (batchTime - lastTime) / duration else 1
        val yeta = math.pow(1 - rate , batchDelta)
        val nwt_ = matrix(wid)
        Range(0, K).map{
          i =>
            deltaVec(i) += (beta - nwt_(i) * yeta)
            (wid, i, beta - nwt_(i))
        }
    }.flatMap(_.toSeq)

    Glint.pushData(deltaMatrix, priorWordTopicCountMat)
    redisVocab.updateVocabBatchTime(rows, batchTime)
    lock.releaseLock(token + "-lazyClear")
    deltaVec
  }

  def lazyClearTopicPararmeters(deltaVec: Array[Double], batchTime: Long, token: String): Unit = {

    lock.fetchLock(token + "-lazyClear")
    Glint.pushData(deltaVec, priorTopicCountVec)
    redisVocab.updateBatchTime(batchTime)

    check(token + "-lazyClear")
    lock.releaseLock(token+ "-lazyClear")
  }

  /**
    * 当词在语料中出现,为了使得词的参数为最新状态时调用
    * @param rawWordIds
    * @param batchTime
    */
  def lazySyncParameter(rawWordIds: Array[Long], batchTime: Long, token: String): Unit = {
    redisVocab.fetchLock(token + "-lazySync")
    val posWordIds = rawWordIds.filter(_ >= 0)
    val (lastBatchTime, lastUpdateTimes) = redisVocab.getLastUpdateBatchTime(posWordIds)

    val wordIdTimes = posWordIds.zip(lastUpdateTimes).filter {
      case (wid, lastTime) =>
        assert(lastTime <= batchTime)
        lastTime < batchTime
    }

    lazySyncParameter(wordIdTimes, lastBatchTime, batchTime, token + "-lazySync")

    check( token + "-lazySync")
    redisVocab.relaseLock(token + "-lazySync")
  }


  private def lazySyncParameter(wordIdTimes: Array[(Long, Long)], lastBatchTime: Long, batchTime: Long, token: String) = {

    //lock.fetchLock(token)
    val wordIds = wordIdTimes.map(_._1)
    val priorNwt = Glint.pullData(wordIds, priorWordTopicCountMat)
    val priorNt = Glint.pullData((0L until K.toLong).toArray, priorTopicCountVec)

    val deltaVec = Array.fill[Double](K)(0)
    if (lastBatchTime < batchTime) {
      val batchDelta = if (lastBatchTime >= 0) (batchTime - lastBatchTime) / duration else 1
      val yeta = math.pow(1 - rate , batchDelta)
      // nt = math.pow(rate, batchDelta) * nt
      for (i <- 0 until K) {
        deltaVec(i) += (yeta - 1) * priorNt(i)
      }
      Glint.pushData(deltaVec, priorTopicCountVec)
      Range(0, priorNt.length).foreach(k=> priorNt(k) *= yeta)
    }

    var minusCount = 0.0
    val deltaNwt = wordIdTimes.filter{
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
    Glint.pushData(deltaNwt, priorWordTopicCountMat)
    redisVocab.updateBatchTime(wordIds, batchTime)

    //lock.releaseLock(token)
    (priorNt, priorNwt)
  }

  /**
    * 将词Id对应的参数更新为最新的状态
    * @param wordIds
    * @param batchTime 当前时间
    * @return
    */
  def lazyFetchParameter(wordIds: Array[Long], batchTime: Long, token: String):
  (Array[Double], Map[Long, Array[Double]]) = {
    val (lastBatchTime , lastUpdateTimes) = redisVocab.getLastUpdateBatchTime(wordIds)
    val wordIdTimes = wordIds.zip(lastUpdateTimes)
    lazySyncParameter(wordIdTimes, lastBatchTime, batchTime, token + "-lazyFetch")
  }


  def fetchParameter(rawWordIds: Array[Long], batchTime: Long): (Array[Double], Map[Long, Array[Double]]) = {
    val wordIds = rawWordIds
    val priorNwt = Glint.pullData(wordIds, priorWordTopicCountMat)
    val priorNt = Glint.pullData((0L until K.toLong).toArray, priorTopicCountVec)
    (priorNt, priorNwt)
  }


  def update(wordTopicCounts: Seq[(Long, Int, Double)],
             time: Long,
             token: String) = {
    lock.fetchLock(token + "-update")
    val wordTopicDelta = wordTopicCounts.map{
      case (wid, topic, cnt) =>
        (wid, topic, cnt * rate)
    }

    check(token + "-before-update")
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

    check(token + "-update")
    lock.releaseLock(token + "-update")
  }

}

