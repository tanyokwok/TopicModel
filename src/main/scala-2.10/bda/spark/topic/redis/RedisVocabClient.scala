package bda.spark.topic.redis


import java.util

import redis.clients.jedis._

import collection.JavaConversions._
import scala.collection.immutable.HashSet

/**
  * Created by Roger on 17/3/5.
  */
class RedisVocabClient(val maxVocabSize: Long,
                       val jedisCluster: JedisCluster,
                       val expired: Long)
                        extends Serializable{

  val vocabKey = "lda.vocab"
  val timeKey = "lda.vocab.age"
  val lockKey = "lda.vocab.lock"
  val countKey = "lda.vocab.count"
  val batchWordKey = "lda.vocab.batch.word"
  val batchKey = "lda.vocab.batch"

  val lock = new RedisLock(jedisCluster, lockKey, expired)

  def clear() {
    jedisCluster.del(vocabKey)
    jedisCluster.del(timeKey)
    jedisCluster.del(countKey)
    jedisCluster.del(lockKey)
    jedisCluster.del(batchWordKey)
    jedisCluster.del(batchKey)
  }

  def loadVocab = jedisCluster.hgetAll(vocabKey).map(x=>(x._2.toLong, x._1))

  /**
    * 如果找到则返回ID, 并打上最新时间戳
    * @param term
    * @param time
    * @return
    */
  def getTerm(term: String, time: Long): Long ={
    val value = jedisCluster.hget(vocabKey, term)

    val termId: Long = if (value == null) {
      -1L
    } else {
      val lastTime = jedisCluster.zscore(timeKey, term)
      if (lastTime.toLong < time) {
        jedisCluster.zadd(timeKey, time.toDouble, term)
      }
      value.toLong
    }

    termId
  }

  /**
    * 增加使用计数
    * @param term
    */
  def incUseCount(term: String): Unit = {
      jedisCluster.hincrBy(countKey, term, 1)
  }

  /**
    * 减少使用计数
    * @param term
    */
  def decUseCount(term: String): Unit ={
      jedisCluster.hincrBy(countKey, term, -1)
  }

  def fetchLock(token: String): Unit = {
    lock.fetchLock(token)
  }

  def relaseLock(token: String): Unit = {
    lock.releaseLock(token)
  }


  def addTerm(term: String, time: Long): Long = {
    val ret = addOrReplace(term, time)

    if (ret >= 0) {
      jedisCluster.zadd(timeKey, time.toDouble, term)
      jedisCluster.hset(countKey, term, "0")
    }

    ret
  }

  def vocabSize = jedisCluster.hlen(vocabKey)

  /**
    * 尝试将词汇添加到词表中,如果词表已经满了的话则激活替换策略, 替换失败返回-1
    * @param term 需要添加的词汇
    * @param time 词汇出现的时间(ms)
    * @return 词汇ID
    */
  private def addOrReplace(term: String, time: Long): Long ={
    val curVocabSize = vocabSize
    if (curVocabSize < maxVocabSize) {
      val ret: Long = jedisCluster.hsetnx(vocabKey, term, curVocabSize.toString)
      if (ret == 0) {
        jedisCluster.hget(vocabKey, term).toLong
      } else {
        curVocabSize
      }
    } else {
      //1. 找到年龄最小的term, termId

      val result = findOneMatch(time)
      if (result == null) {
        println("[WARNING] The vocabulary size is too small, please set a bigger one!")
        return -1L
      }
      val leastUsedTerm = result.getElement
      val leastUserTime = result.getScore
      //println(s"[INFO] $leastUsedTerm is age $age")

      val leastUsedTermId = jedisCluster.hget(vocabKey, leastUsedTerm)

      //2. 将term, termId从Vocab中删除
      if (jedisCluster.hdel(vocabKey, leastUsedTerm) == 0) {
        println(s"[ERROR] jedis.hdel(vocabKey,${leastUsedTerm}) failed!")
        System.exit(-1)
      }

      if (jedisCluster.zrem(timeKey, leastUsedTerm) == 0) {
        println(s"[ERROR] jedis.zrem(timeKey,${leastUsedTerm}) failed!")
        System.exit(-1)
      }

      if (jedisCluster.hdel(countKey, leastUsedTerm) == 0) {
        println(s"[ERROR] jedis.hdel(countKey,${leastUsedTerm}) failed!")
        System.exit(-1)
      }

      jedisCluster.hset(vocabKey, term, leastUsedTermId)
      leastUsedTermId.toLong
    }
  }

  /**
    * 全局测试(按照最迟访问时间递增)查找没有被使用过的词汇<br/>
    * 如果发现被测时间已经在当前时间之后,则返回null
    * @param time 当前时间
    * @return 查找命中的对象
    */
  private def findOneMatch(time: Long):Tuple = {

    var index = 0
    val batch = 10
    while (true) {
      val result: util.Set[Tuple] = jedisCluster.zrangeWithScores(timeKey, index * batch, (index + 1) * batch )
      for (entry <- result) {
        val leastUsedTerm = entry.getElement
        val leastUsedTime= entry.getScore

        if (leastUsedTime.toLong >= time) {
          return null
        }

        val count = jedisCluster.hget(countKey, leastUsedTerm)

        assert( count == null || count.toLong >= 0)

        if (count == null || count.toLong == 0) {
          return entry
        }
      }
      index += 1
    }
    return null
  }

  override def finalize(): Unit ={
    close()
  }

  private def close(): Unit = {
    jedisCluster.close()
  }

}


object RedisVocabClient {

  def apply(maxSize: Long = 1000000L,
            jedis: JedisCluster):RedisVocabClient = {
    val client = new RedisVocabClient(maxSize, jedis, 60000)
    client
  }

}