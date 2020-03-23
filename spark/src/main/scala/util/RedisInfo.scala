package util

import scala.beans.BeanProperty

class RedisInfo extends Serializable {
  @BeanProperty var redisName:String = _
  @BeanProperty var ip:String = _
  @BeanProperty var port:String = _
  @BeanProperty var pwd:String = _
  @BeanProperty var maxRedirects:String = _
  @BeanProperty var readTimeout:String = _
  @BeanProperty var connectTimeout:String = _
  @BeanProperty var templastesInit:String = _
  @BeanProperty var templastesUse:String = _
  @BeanProperty var description:String = _
  override def toString = s"集群信息RedisInfo($redisName,$ip,$port,$pwd,$maxRedirects,$readTimeout,$connectTimeout,$templastesInit,$templastesUse,$description)"
}
