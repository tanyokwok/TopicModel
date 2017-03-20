package bda.spark.topic.redis

import java.util

import org.scalatest.FunSuite
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster, Response}

import scala.collection.JavaConversions._
import scala.collection.immutable.HashSet

/**
  * Created by Roger on 17/3/6.
  */
class RedisVocabSuite extends FunSuite{

  test("RedisVocabPipeline") {
    val jedis = new Jedis("bda04", 6379)
    val jedis2 = new Jedis("bda04", 6379)
    val redisLock = new RedisLock(jedis2, "lock", 6000)
    val client = new RedisVocab(8L, jedis, redisLock, 6000)
    client.clear()
    assert(client.vocabSize == 0)

    println("Begin test")
    val terms = Array("Roger", "Gorge", "Hinton", "Mary", "Jack")
    println("get term ids")
    var ids = client.getTermIds(terms, 0)
    assert( ids.sum == -5)
    println("add terms")
    client.fetchLock("lock")
    ids = client.addTerms(terms, 0)
    client.incUseCount(terms)
    assert( ids.sum == 10)
    assert(ids(0) == 0 && ids(1) == 1)

    client.relaseLock("lock")
    client.fetchLock("lock")
    println(client.loadVocab.toArray.sortBy(_._2).mkString(" "))
    val terms2 = Array("Alex", "Xing", "Peter", "Mike")
    ids = client.addTerms(terms2, 1)
    client.incUseCount(terms2)
    println(client.loadVocab.toArray.sortBy(_._2).mkString(" "))

    client.decUseCount(terms2)

    client.relaseLock("lock")
    val terms3 = Array("Miller", "Tiger", "Woods", "James", "Kobe")
    client.decUseCount(terms3)

    ids = client.addTerms(terms3, 2)

    client.incUseCount(terms3)

    println(ids)
    println(client.loadVocab.toArray.sortBy(_._2).mkString(" "))

    client.decUseCount(terms)
    client.decUseCount(terms3)
    client.clear()

  }
}
