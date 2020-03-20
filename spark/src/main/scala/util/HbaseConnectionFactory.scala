package util

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.slf4j.LoggerFactory

import scala.beans.BeanProperty

class HbaseConnectionFactory extends Serializable {
  val log  = LoggerFactory.getLogger(getClass)

  @BeanProperty
  var connection: HBaseConnection = _
  @BeanProperty
  var configuration: HBaseConfiguration = _
  @BeanProperty
  var admin: Admin = _
  //初始化创建
  def init():Unit = {
    createConnection
    admin = connection.getAdmin()
  }

  /**
   * 创建链接
   * @return
   */
  def createConnection():HBaseConnection = {
    try {
      val configration = HBaseConfiguration.create()
      val connection = ConnectionFactory.createConnection(configration)
      val conn = new HBaseConnection(connection)
      this.connection = conn
      conn
    }catch {
      case e:Exception =>{
        throw e
      }
    }
  }
}
