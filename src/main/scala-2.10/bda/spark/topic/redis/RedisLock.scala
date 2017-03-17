package bda.spark.topic.redis

import redis.clients.jedis.{Jedis, JedisCluster, JedisCommands}

import collection.JavaConversions._
/**
  * Created by Roger on 17/3/10.
  */
class RedisLock(jedis: JedisCommands,
                lockKey: String,
                expired: Long) {

  def clear(): Unit ={
    if (jedis != null){
      jedis.del(lockKey)
    }
    else{
      jedis.del(lockKey)
    }
  }

  def fetchLock(token: String): Unit = {
    while (jedis.setnx(lockKey, token) == 0) {
      Thread.sleep(100)
      //println(s"$token wait for lock")
    }
    jedis.expire(lockKey, expired.toInt)
    println(s"$token fetch lock $lockKey")
  }

  def releaseLock(token: String): Unit = {
    if (jedis.get(lockKey) == token) {
      jedis.del(lockKey)
      println(s"$token release lock $lockKey")
    }
  }

  def close(): Unit ={
    if (jedis.isInstanceOf[Jedis]) {
      jedis.asInstanceOf[Jedis].close()
    } else {
      jedis.asInstanceOf[JedisCluster].close()
    }
  }
  override def finalize(): Unit = {
    close()
  }
}
