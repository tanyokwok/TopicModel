package bda.spark.topic.redis

import org.scalatest.FunSuite
import redis.clients.jedis.{HostAndPort, JedisCluster}

import scala.collection.immutable.HashSet
import collection.JavaConversions._
/**
  * Created by Roger on 17/3/6.
  */
class RedisVocabClientSuite extends FunSuite{

  test("RedisVocabClient") {

    val jedisNodes = HashSet[HostAndPort](new HostAndPort("bda07", 30001))
    val jedis = new JedisCluster(jedisNodes)
    val client = RedisVocabClient( 2L, jedis)
    client.clear()
    assert( client.vocabSize == 0)
    var gorgeId = client.getTerm("gorge", 0L)
    assert(gorgeId == -1)
    gorgeId = client.addTerm("gorge", 0L)
    assert(gorgeId == 0)
    val maryId = client.addTerm("mary", 1L)
    val rogerId = client.addTerm("roger", 1L)
    assert( maryId == 1)
    assert( rogerId == 0)

    gorgeId = client.addTerm("gorge", 1L)

    assert(gorgeId == -1)
  }
}
