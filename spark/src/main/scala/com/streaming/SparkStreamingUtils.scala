package com.streaming

import org.apache.commons.lang.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies, OffsetRange}
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
    if (!StringUtils.isBlank(conf)) {
      for (kv <- conf.split("@@")) {
        var arr = kv.split("=")
        sparkConf.set(arr(0), arr(1))
      }
    }

    val scc = new StreamingContext(sparkConf, Seconds(bacthtime))
    if (!StringUtils.isBlank(checkpoint) && !"defaultCheckpoint".equals(checkpoint)) {
      scc.checkpoint(checkpoint)
    }
    scc
  }

  def getDataByKafka(caseConf: CaseConf, scc: StreamingContext): InputDStream[ConsumerRecord[String, String]]={
    val ip = caseConf.get("metadata.broker.list")
    val groupId = caseConf.get("group.id")
    val kafkaParams=Map("metadata.broker.list"->ip,"group.id"->groupId)
    val topicsSet=caseConf.get("input.kafka.name").split(",").toSet
    val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      scc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
    )
//    val lines: DStream[String] = dstream.map(_.value)
    dstream
  }

  def getCaseConf(args: Array[String], dir: String):CaseConf = {
    var caseConf: CaseConf = null
    if(dir.contains("HadoopAPARK")){
      val fileName = args(0)
      val path = "file:///" + dir + "\\resources\\" + fileName
      caseConf = new CaseConf(path)
    }else{
      caseConf = new CaseConf(args(0))
    }
    caseConf
  }

  def saveOffset(offsetRange: Array[OffsetRange], broadcastConf: Broadcast[CaseConf]) ={

  }
}