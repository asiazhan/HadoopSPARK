package util


import java.util

import org.apache.commons.lang3.StringUtils
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.slf4j.LoggerFactory
import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPoolConfig}

import scala.collection.mutable


class RedisConnectionFactory(resiaInfoMap: mutable.Map[String, RedisInfo]) extends Serializable {
  val log  = LoggerFactory.getLogger(getClass)

  /**
   * 创建集群链接
   * @param redisName
   * @param poolConfig
   * @param redisInfo
   * @return
   */
  private def createCluster(redisName: String, poolConfig: GenericObjectPoolConfig[Object], redisInfo: RedisInfo): JedisCluster = {
    var nodes = Array[String]()
    var jedisCluster: JedisCluster = null
    try {
      val passwordTmp = redisInfo.getPwd
      val maxRedirectsTmp = redisInfo.getMaxRedirects
      val readTimeoutTmp = redisInfo.getReadTimeout
      val connectTimeoutTmp = redisInfo.getConnectTimeout
      var pw = ""
      //最大重试次数
      var maxRedirects = "5"
      //命令响应时间 ms
      var readTimeout = "18000"
      //链接server超时时间
      var connectTimeout = "18000"
      if(!StringUtils.isBlank(passwordTmp)){
        pw = passwordTmp
      }
      if(!StringUtils.isBlank(maxRedirectsTmp)){
        maxRedirects = maxRedirectsTmp
      }
      if(!StringUtils.isBlank(readTimeoutTmp)){
        readTimeout = readTimeoutTmp
      }
      if(!StringUtils.isBlank(connectTimeoutTmp)){
        connectTimeout = connectTimeoutTmp
      }
      //ip
      nodes = redisInfo.getIp.split(",")
      //端口列表
      val ports = redisInfo.getPort.split(",")
      val hostAndPort: util.Set[HostAndPort]  = new util.HashSet[HostAndPort]
      //端口和IP组合
      for(node <- nodes){
        for(port <- ports){
          hostAndPort.add(new HostAndPort(node,port.toInt))
        }
      }
      if(!StringUtils.isBlank(pw)){
        jedisCluster = new JedisCluster(hostAndPort,connectTimeout.toInt,readTimeout.toInt,maxRedirects.toInt,pw,redisName,poolConfig)
      }else{
        jedisCluster = new JedisCluster(hostAndPort,connectTimeout.toInt,readTimeout.toInt,maxRedirects.toInt,poolConfig)

      }
    }catch {
      case e:Exception =>{
        log.info(s"init jedis faild:${redisName},${redisInfo}")
        throw e
      }
    }
    jedisCluster
  }
  def createByName(redisName: String): RdisConnection = {
    val poolConfig = new JedisPoolConfig
    val redisInfo = resiaInfoMap(redisName)
    val redisInfoObject = redisInfo.asInstanceOf[RedisInfo]
    val clusterTable = createCluster(redisName, poolConfig.asInstanceOf, redisInfoObject)
    return new RdisConnection(clusterTable)
  }
}
