package bda.spark.topic.redis

import org.scalatest.FunSuite

/**
  * Created by Roger on 17/3/6.
  */
class RedisVocabClientSuite extends FunSuite{

  test("RedisVocabClient") {
    RedisVocabClient.clear("localhost", 30001)
    val client = RedisVocabClient("localhost", 30001, 2L)
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
