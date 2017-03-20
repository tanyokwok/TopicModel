package bda.spark.topic.redis

import redis.clients.jedis._

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Roger on 17/3/14.
  */
class RedisPoolVocab(val maxVocabSize: Long,
                     val jedisPool: JedisPool,
                     val lock: RedisPoolLock,
                     expired: Long)
  extends Serializable {

  val vocabKey = "lda.vocab"
  val timeKey = "lda.vocab.age"
  val countKey = "lda.vocab.count"
  val batchWordKey = "lda.vocab.batch.word"
  val batchKey = "lda.vocab.batch"
  val lockKey = "lda.vocab.lock"

  def clear() {
    var jedis: Jedis = null
    try {
      jedis = jedisPool.getResource
      val pipeline = jedis.pipelined()
      pipeline.del(Array(vocabKey): _*)
      pipeline.del(Array(timeKey): _*)
      pipeline.del(Array(countKey): _*)
      pipeline.del(Array(batchWordKey): _*)
      pipeline.del(Array(batchKey): _*)
      pipeline.sync()
      pipeline.close()
    } finally {
      if (jedis != null) jedis.close()
    }

    lock.clear()
    assert(vocabSize == 0)
  }

  def loadVocab = {
    var jedis: Jedis = null
    try {
      jedis = jedisPool.getResource
      val vocab = jedis.hgetAll(vocabKey)

      vocab.map(x => (x._2.toLong, x._1))
    } finally {
      if (jedis != null) jedis.close()
    }
  }

  def vocabSize = {
    var jedis: Jedis = null
    try {
      jedis = jedisPool.getResource
      jedis.hlen(vocabKey)
    } finally {
      if (jedis != null) jedis.close()
    }
  }

  def updateVocabBatchTime(ids: Array[Long], time: Long): Unit = {
    var jedis: Jedis = null
    try {
      jedis = jedisPool.getResource
      val pipeline = jedis.pipelined()
      ids.map {
        id =>
          pipeline.hset(batchWordKey, id.toString, time.toString)
      }
      pipeline.sync()
      pipeline.close()
    } finally {
      if (jedis != null) jedis.close()
    }

  }

  def updateBatchTime(time: Long): Unit = {
 var jedis: Jedis = null
    try {
      jedis = jedisPool.getResource
      val pipeline = jedis.pipelined()
      pipeline.set(batchKey, time.toString)
      pipeline.sync()
      pipeline.close()
    } finally {
      if (jedis != null) jedis.close()
    }

  }

  def updateBatchTime(ids: Array[Long], time: Long): Unit = {
    var jedis: Jedis = null
    try {
      jedis = jedisPool.getResource
      val pipeline = jedis.pipelined()
      ids.map {
        id =>
          pipeline.hset(batchWordKey, id.toString, time.toString)
      }
      pipeline.set(batchKey, time.toString)
      pipeline.sync()
      pipeline.close()
    } finally {
      if (jedis != null) jedis.close()
    }

  }

  def getLastUpdateBatchTime(ids: Array[Long]): (Long, Seq[Long]) = {
    var jedis: Jedis = null
    try {
      jedis = jedisPool.getResource
      val pipeline = jedis.pipelined()

      val reps = ids.map {
        id =>
          pipeline.hget(batchWordKey, id.toString)
      }
      val ret = pipeline.get(batchKey)
      pipeline.sync()

      pipeline.close()

      (if (ret.get() == null) -1L else ret.get().toLong,
        reps.map(x => if (x.get() == null) -1L else x.get().toLong))


    } finally {
      if (jedis != null) jedis.close()
    }
  }

  def getTermIds(terms: Array[String], time: Long): Seq[Long] = {
    var jedis: Jedis = null
    try {
      jedis = jedisPool.getResource
      val pipeline = jedis.pipelined()

      val vocabResps = terms.map {
        term =>
          pipeline.hget(vocabKey, term)
      }
      pipeline.sync()

      val termIds = vocabResps.map {
        resp =>
          if (resp.get() == null) -1L
          else resp.get().toLong
      }

      val termNeedUpdate = terms.zip(termIds).filter {
        case (term, id) =>
          id >= 0
      }.map(_._1)

      val timeResps = termNeedUpdate.map {
        term =>
          pipeline.zscore(timeKey, term)
      }
      pipeline.sync()

      timeResps.zip(termNeedUpdate).foreach {
        case (resp, term) =>
          if (resp.get().toLong < time) {
            pipeline.zadd(timeKey, time.toDouble, term)
          }
      }
      pipeline.sync()
      pipeline.close()
      termIds
    } finally {
      if (jedis != null) jedis.close()
    }
  }

  private def addUseCount(terms: Array[String], count: Int): Unit = {
    var jedis: Jedis = null
    try {
      jedis = jedisPool.getResource
      val pipeline = jedis.pipelined()
      terms.foreach {
        term =>
          pipeline.hincrBy(countKey, term.toString, count)
      }
      pipeline.sync()
      pipeline.close()
    } finally {
      if (jedis != null) jedis.close()
    }
  }

  def incUseCount(terms: Array[String]): Unit = {
    addUseCount(terms, 1)
  }

  def decUseCount(terms: Array[String]): Unit = {
    addUseCount(terms, -1)
  }

  def addTerms(terms: Array[String], time: Long): Seq[Long] = {
    addOrReplace(terms, time)
  }


  def fetchLock(token: String): Unit = {
    lock.fetchLock(token)
  }

  def relaseLock(token: String): Unit = {
    lock.releaseLock(token)
  }

  private def newTerms(termIds: Seq[(String, Long)], time: Long, pipeline: Pipeline): Unit = {
    termIds.foreach {
      case (term, id) =>
        pipeline.hset(vocabKey, term, id.toString)
        pipeline.hset(countKey, term, "0")
        pipeline.zadd(timeKey, time.toDouble, term)
    }
  }

  private def addOrReplace(terms: Array[String], time: Long): Seq[Long] = {

    var ret: Seq[Long] = Seq[Long]()
    val curVocabSize: Long = vocabSize
    var restSize: Long = maxVocabSize - curVocabSize
    var jedis: Jedis = null

    try {
      jedis = jedisPool.getResource
      val pipeline = jedis.pipelined()

      if (restSize > 0) {
        val batch1terms = terms.slice(0, restSize.toInt)
        ret = ret ++ (curVocabSize until (curVocabSize + batch1terms.length))
        newTerms(batch1terms.zip(ret), time, pipeline)
      } else {
        restSize = 0
      }

      val termsNeedReplace = terms.slice(restSize.toInt, terms.length)

      val matches: ArrayBuffer[String] = findMatch(termsNeedReplace.length, time)
      val termsLucky = termsNeedReplace.slice(0, matches.length)
      val less = termsNeedReplace.length - matches.length

      if (matches.length > 0) {
        val reps = matches.map {
          term =>
            pipeline.hget(vocabKey, term)
        }
        pipeline.sync()

        val ids = reps.map(_.get().toLong)
        val luckTermId = termsLucky.zip(ids)
        matches.zip(luckTermId).foreach {
          case (origin, (target, id)) =>
            pipeline.hdel(vocabKey, origin)
            pipeline.zrem(timeKey, origin)
            pipeline.hdel(countKey, origin)
        }

        newTerms(luckTermId, time, pipeline)
        pipeline.sync()
        ret = ret ++ ids
      } else {
        pipeline.sync()
      }
      pipeline.sync()
      pipeline.close()
      return ret ++ Array.fill[Long](less)(-1L)
    } finally {
      if (jedis != null) jedis.close()
    }
  }

  private def findMatch(needSize: Int, time: Long): ArrayBuffer[String] = {
    var jedis: Jedis = null
    try {
      jedis = jedisPool.getResource
      val pipeline = jedis.pipelined()
      val ret = new ArrayBuffer[String]()
      if (needSize == 0) {
        return ret
      }
      var offset = 0
      var reqSize = needSize * 2

      var flag = true
      //val countPipeline = getJedis(countKey).pipelined()
      while (flag) {
        //获取[offset, reqSize)区间内的word
        //val result = jedis.zrangeWithScores(timeKey, offset, reqSize)
        val rep = pipeline.zrangeWithScores(timeKey, offset, offset + reqSize)
        pipeline.sync()
        val result = rep.get()
        if (result.size() == 0) {
          flag = false
        } else {
          val candidates = new ArrayBuffer[String]()
          for (tuple <- result if (flag)) {
            val elem = tuple.getElement
            val score = tuple.getScore

            // to make sure the latest updated words not be replace
            if (score.toLong >= time) {
              //在此之后的score都>= time
              flag = false
            } else {
              candidates += elem
            }
          }

          //过滤出没有被使用的词
          val reps = candidates.map {
            cand =>
              pipeline.hget(countKey, cand)
          }
          pipeline.sync()

          ret ++= candidates.zip(reps).filter {
            case (term, rep) =>
              rep.get() == null || rep.get().toLong == 0
          }.map(_._1)

          //更新offset和reqSize
          offset += reqSize
          if (ret.length >= needSize) {
            //已经找到足够多的结果
            flag = false
          } else {
            reqSize = needSize - ret.length
            reqSize *= 2
          }
        }
      }

      pipeline.close()
      return ret.slice(0, needSize)
    } finally {
      if (jedis != null) jedis.close()
    }
  }


}
