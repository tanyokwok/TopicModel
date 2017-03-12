package bda.spark.topic.redis

import java.util

import redis.clients.jedis.JedisCluster

/**
  * Created by Roger on 17/3/10.
  */
class RedisLock(jedis: JedisCluster,
                lockKey: String = "lock") {
  def fetchLock(token: String): Unit = {
    while (jedis.setnx(lockKey, token) == 0) {
      Thread.sleep(100)
      //println(s"$token wait for lock")
    }
    println(s"$token fetch lock")
  }

  def releaseLock(token: String): Unit = {
    if (jedis.get(lockKey) == token) {
      jedis.del(lockKey)
      println(s"$token release lock")
    }
  }
}
