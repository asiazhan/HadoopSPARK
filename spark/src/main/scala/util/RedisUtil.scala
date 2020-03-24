package util

import java.util.concurrent.ConcurrentHashMap

object RedisUtil {
  private val redisClusters = new ConcurrentHashMap[String,RdisConnection]()
  private var connectionFactory: RedisConnectionFactory = null
  private val initLock = new Object


  def getConnectionByRredis(redisName: String): RdisConnection = {
    try {
     var conn = redisClusters.get(redisName)
      if(conn == null){
        initLock.synchronized{
          conn = redisClusters.get(redisName)
          if(conn == null){
            conn = connectionFactory.createByName(redisName)
          }
        }
      }
      conn
    }catch {
      case e: Exception => {
        throw e
      }
    }
  }

}
