package bda.spark.topic.redis

import org.apache.spark.Logging
import redis.clients.jedis.{Jedis, JedisCluster, JedisCommands, JedisPool}

/**
  * Created by Roger on 17/3/10.
  */
class RedisPoolLock(jedisPool: JedisPool,
                    lockKey: String,
                    expired: Long) extends Logging{


  def clear(): Unit ={
    var jedis:Jedis = null
    try{
      jedis = jedisPool.getResource
      jedis.del(lockKey)
    }
  }

  def fetchLock(token: String): Unit = {
    var jedis: Jedis = null
    var flag = true
    while (flag) {
      try {
        jedis = jedisPool.getResource
        if (jedis.setnx(lockKey, token) != 0) {
          jedis.expire(lockKey, expired.toInt)
          logInfo(s"$token fetch lock $lockKey")
          flag = false
        }
      } finally {
        if (jedis != null) jedis.close()
      }

      if (flag) {
        Thread.sleep(100)
      }
    }
 }

  def releaseLock(token: String): Unit = {
    var jedis: Jedis = null
    try {
      jedis = jedisPool.getResource
      if (jedis.get(lockKey) == token) {
        jedis.del(lockKey)
        logInfo(s"$token release lock $lockKey")
      }
    } finally{
      if (jedis != null) jedis.close()
    }

  }

}
