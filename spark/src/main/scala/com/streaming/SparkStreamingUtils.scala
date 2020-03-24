package com.streaming

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.CaseConf

object SparkStreamingUtils extends Serializable {
  /**
   * 创建上下文
   *
   * @param caseConf
   * @return
   */
  def createStringContext(caseConf: CaseConf): StreamingContext = {
    val checkpoint = caseConf.get("checkpoint", "defaultCheckpoint")
    val appname = caseConf.get("appname", "defaultappname")
    val bacthtime = caseConf.get("bacthtime", "dafaultbacthtime").toInt
    val sparkConf = new SparkConf()
      .setAppName(appname)
      .setIfMissing("spark.master", "local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[CaseConf]))
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", "2000")

    val conf = caseConf.get("spark.conf", "")
    if (StringUtils.isNoneBlank(conf)) {
      for (kv <- conf.split("@@")) {
        var arr = kv.split("=")
        sparkConf.set(arr(0), arr(1))
      }
    }

    val scc = new StreamingContext(sparkConf, Seconds(bacthtime))
    if (StringUtils.isNoneBlank(checkpoint) && !"defaultCheckpoint".equals(checkpoint)) {
      scc.checkpoint(checkpoint)
    }
    scc
  }
}