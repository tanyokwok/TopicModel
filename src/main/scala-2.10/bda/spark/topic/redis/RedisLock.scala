package bda.spark.topic.redis

import redis.clients.jedis.JedisCluster
import collection.JavaConversions._
/**
  * Created by Roger on 17/3/10.
  */
class RedisLock(jedis: JedisCluster,
                lockKey: String,
                expired: Long) {

  def clear(): Unit ={
    jedis.del(lockKey)
  }

  def fetchLock(token: String): Unit = {
    while (jedis.set(lockKey, token, "NX", "PX", expired) == 0) {
      Thread.sleep(100)
      //println(s"$token wait for lock")
    }
    //println(s"$token fetch lock $lockKey")
  }

  def releaseLock(token: String): Unit = {
    if (jedis.get(lockKey) == token) {
      jedis.del(lockKey)
     // println(s"$token release lock $lockKey")
    }
  }

  def close(): Unit ={
    jedis.close()
  }
  override def finalize(): Unit = {
    close()
  }
}
