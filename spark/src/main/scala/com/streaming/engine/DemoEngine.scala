package com.streaming.engine

import com.streaming.SparkStreamingUtils
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.slf4j.LoggerFactory
import util.CaseConf

class DemoEngine extends Serializable {
  val log  = LoggerFactory.getLogger(getClass)

  def run(args: Array[String], dir: String) = {
    var offsetsArray = Array[OffsetRange]()
    try {
      if(args == null || args.isEmpty){
        log.info("本地调试，带传入参数")
        val conf = SparkStreamingUtils.getCaseConf(args, dir)
        val sc = SparkStreamingUtils.createStringContext(conf)
        val lines = SparkStreamingUtils.getDataByKafka(conf, sc)
        val scConf = sc.sparkContext.broadcast(conf)
        lines.foreachRDD{ rdd =>
          offsetsArray = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          //保存最新处理的offset范围
          if(!rdd.isEmpty()){
            val data= rdd.map(_.value)
            data.foreachPartition{iterator =>
              val result = process(iterator, scConf.value)
            }
          }

        }
        sc.start()
        log.info("Spark Streaming 启动成功。。。。。。。。。。。。。")
        sc.awaitTermination()
      }
    }catch {
      case e:Exception =>{
        throw e
      }
    }
  }
  def process(iterator: Iterator[String], caseConf: CaseConf): Iterator[String]
}
