package bda.spark.topic.redis

import org.apache.spark.Logging
import redis.clients.jedis.Pipeline

/**
  * Created by Roger on 17/3/10.
  */
class PipelineLock(pipeline: Pipeline,
                   lockKey: String,
                   expired: Long) extends Logging{


  def clear(): Unit ={
    pipeline.del(Array(lockKey):_*)
    pipeline.sync()
  }

  def fetchLock(token: String): Unit = {
    var flag = true
    while (flag) {
      val ret = pipeline.setnx(lockKey, token)
      pipeline.sync()
      if (ret.get() == 0) {
        Thread.sleep(100)
      } else {
        flag = false
      }
    }
    pipeline.expire(lockKey, expired.toInt)
    pipeline.sync()

    logInfo(s"$token fetch lock $lockKey")
  }

  def releaseLock(token: String): Unit = {
    val ret = pipeline.get(lockKey)
    pipeline.sync()
    if (ret.get() == token) {
      pipeline.del(Array(lockKey):_*)
      pipeline.sync()
    }
    logInfo(s"$token release lock $lockKey")
  }

}
