package util

import redis.clients.jedis.JedisCluster

class RdisConnection(redisCluster: JedisCluster) extends RedisCommands{

  private val cluster_ : JedisCluster = redisCluster

  def cluster = cluster_
  override def set(key: String, value: String): String = cluster.set(key,value)

  override def get(key: String): String = cluster.get(key)

  override def existe(key: String): Boolean = cluster.exists(key)

  override def expire(key: String, seconds: Int): Long = cluster.expire(key, seconds)

  override def pexpire(key: String, milliseconds: Long): Long = cluster.pexpire(key, milliseconds)
}
