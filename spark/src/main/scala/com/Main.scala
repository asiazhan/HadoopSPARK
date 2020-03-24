package com

import com.streaming.SparkStreamingUtils
import org.apache.spark.{SparkConf, SparkContext}
import util.CaseConf

/**
  * Created by Administrator on 2019/7/22.
  */
object Main {
  def main(args: Array[String]): Unit = {
    //获取集群入口
    val conf = new CaseConf("ghdhjdjk")
    val sc = SparkStreamingUtils.createStringContext(conf)
    val lines = SparkStreamingUtils.getDataByKafka(conf, sc)

  }
}
