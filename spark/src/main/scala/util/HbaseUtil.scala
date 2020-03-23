package util

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.slf4j.LoggerFactory

object HbaseUtil {
  val log  = LoggerFactory.getLogger(getClass)
  private var hbaseConnectionFactory : HbaseConnectionFactory = _
  private var initialized = false
  private val initLock = new Object

  def initialize(conf:CaseConf = null, createFlag: Boolean = false) = {
    if(!initialized){
      initLock.synchronized{
        if(!initialized){
          hbaseConnectionFactory = new HbaseConnectionFactory
          hbaseConnectionFactory.init()
//          if(createFlag){
//
//          }
          initialized = true
        }
      }
      log.info("Hbase 初始化成功")
    }
  }

}
