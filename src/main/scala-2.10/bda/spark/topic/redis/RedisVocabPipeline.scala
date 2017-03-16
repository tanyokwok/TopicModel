package bda.spark.topic.redis

import java.util
import java.util.Map.Entry

import redis.clients.jedis._
import redis.clients.jedis.exceptions.JedisClusterException
import redis.clients.util.JedisClusterCRC16
import spire.std.byte

import collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
/**
  * Created by Roger on 17/3/14.
  */
class RedisVocabPipeline(override val maxVocabSize: Long,
                         override val jedisCluster: JedisCluster,
                         expired: Long)
  extends RedisVocabClient(maxVocabSize, jedisCluster, expired) {

  private var nodeMap: util.Map[String, JedisPool] = null
  private var slotHostMap: util.TreeMap[Long, String] = initJedisNodeMap()
  val vocabPipeline = getJedis(vocabKey).pipelined()
  val countPipeline = getJedis(countKey).pipelined()
  val timePipeline = getJedis(timeKey).pipelined()
  val batchPipeline = getJedis(batchWordKey).pipelined()

  def initJedisNodeMap(): util.TreeMap[Long, String] = {
    try {
      nodeMap = jedisCluster.getClusterNodes()
      println(nodeMap.keySet())
      val anyHost = nodeMap.keySet().iterator().next()
      slotHostMap = getSlotHostMap(anyHost);
    } catch {
      case e: JedisClusterException =>
        println(e.getMessage());
    }

    slotHostMap
  }

  def getLastUpdateBatchTime(ids: Array[Long]): (Long, Seq[Long]) = {
    val reps = ids.map{
      id =>
        batchPipeline.hget(batchWordKey, id.toString)
    }
    batchPipeline.sync()

    val ret = jedisCluster.get(batchKey)

    (if (ret == null) -1 else ret.toLong,
      reps.map(_.get().toLong))
  }

  def getTermIds(terms: Array[String], time: Long): Seq[Long] = {
    val vocabResps = terms.map {
      term =>
        vocabPipeline.hget(vocabKey, term)
    }

    vocabPipeline.sync()

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
        timePipeline.zscore(timeKey, term)
    }
    timePipeline.sync()

    timeResps.zip(termNeedUpdate).foreach {
      case (resp, term) =>
        if (resp.get().toLong < time) {
          timePipeline.zadd(timeKey, time.toDouble, term)
        }
    }
    timePipeline.sync()

    termIds
  }

  private def addUseCount(terms: Array[String], count: Int): Unit = {
    terms.foreach {
      term =>
        countPipeline.hincrBy(countKey, term.toString, count)
    }
    countPipeline.sync()
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

  private def newTerms(termIds: Seq[(String, Long)], time: Long): Unit = {
    termIds.foreach {
      case (term, id) =>
        vocabPipeline.hset(vocabKey, term, id.toString)
        countPipeline.hset(countKey, term, "0")
        timePipeline.zadd(timeKey, time.toDouble, term)
        batchPipeline.hset(batchWordKey, id.toString, time.toString)
    }
  }

  private def addOrReplace(terms: Array[String], time: Long): Seq[Long] = {

    var ret: Seq[Long] = Seq[Long]()
    val curVocabSize: Long = vocabSize
    var restSize: Long = maxVocabSize - curVocabSize

    if (restSize > 0) {
      val batch1terms = terms.slice(0, restSize.toInt)
      ret = ret ++ (curVocabSize until (curVocabSize + batch1terms.length))
      newTerms(batch1terms.zip(ret), time)
    } else {
      restSize = 0
    }

    val termsNeedReplace = terms.slice(restSize.toInt, terms.length)

    val matches: ArrayBuffer[String] = findMatch(termsNeedReplace.length, time,
      timePipeline, countPipeline)
    val termsLucky = termsNeedReplace.slice(0, matches.length)
    val less = termsNeedReplace.length - matches.length

    if (matches.length > 0) {
      val reps = matches.map {
        term =>
          vocabPipeline.hget(vocabKey, term)
      }
      vocabPipeline.sync()

      val ids = reps.map(_.get().toLong)
      val luckTermId = termsLucky.zip(ids)
      matches.zip(luckTermId).foreach {
        case (origin, (target, id)) =>
          vocabPipeline.hdel(vocabKey, origin)
          timePipeline.zrem(timeKey, origin)
          countPipeline.hdel(countKey, origin)
      }

      newTerms(luckTermId, time)

      vocabPipeline.sync()
      timePipeline.sync()
      countPipeline.sync()
      batchPipeline.sync()
      ret = ret ++ ids
    } else {
      vocabPipeline.sync()
      timePipeline.sync()
      countPipeline.sync()
      batchPipeline.sync()
    }

    return ret ++ Array.fill[Long](less)(-1L)
  }

  private def findMatch(needSize: Int, time: Long,
                        timePipeline: Pipeline,
                        countPipeline: Pipeline): ArrayBuffer[String] = {
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
      //val result = jedisCluster.zrangeWithScores(timeKey, offset, reqSize)
      val rep = timePipeline.zrangeWithScores(timeKey, offset, offset + reqSize)
      timePipeline.sync()
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
            countPipeline.hget(countKey, cand)
        }

        countPipeline.sync()

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

    return ret.slice(0, needSize)
  }


  private def getJedis(key: String): Jedis = {
    val slot = JedisClusterCRC16.getSlot(key);
    val entry: Entry[Long, String] = slotHostMap.lowerEntry(slot.toLong);
    println(entry.getValue())
    val pool = nodeMap.get(entry.getValue())
    pool.getResource()
  }

  private def getSlotHostMap(anyHostAndPortStr: String): util.TreeMap[Long, String] = {
    val tree = new util.TreeMap[Long, String]();
    val parts = anyHostAndPortStr.split(":");
    val anyHostAndPort = new HostAndPort(parts(0), parts(1).toInt);
    try {
      val jedis = new Jedis(anyHostAndPort.getHost(), anyHostAndPort.getPort())
      val list: util.List[AnyRef] = jedis.clusterSlots()
      for (obj <- list) {
        val list1 = obj.asInstanceOf[util.List[AnyRef]]
        val master = list1(2).asInstanceOf[util.List[AnyRef]]
        val hostAndPort = s"${new String(master(0).asInstanceOf[Array[Byte]])}:${master(1)}";
        tree.put(list1(0).asInstanceOf[Long], hostAndPort);
        tree.put(list1(1).asInstanceOf[Long], hostAndPort);
      }
    }
    return tree;
  }

}
