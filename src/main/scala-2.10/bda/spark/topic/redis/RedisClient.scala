package bda.spark.topic.redis

import java.util

import collection.JavaConversions._
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster}

import scala.collection.immutable.HashSet
/**
  * Created by Roger on 17/3/2.
  */
class RedisClient(val host: String = "localhost",
                  val port: Int = 0) {

  val jedisClusterNodes = HashSet[HostAndPort](new HostAndPort(host, port))
  val jedis = new JedisCluster(jedisClusterNodes)

}

