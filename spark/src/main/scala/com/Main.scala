package com

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2019/7/22.
  */
object Main {
  def main(args: Array[String]): Unit = {
    //获取集群入口
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    //从 HDFS 中读取文件
    val sc = new SparkContext(sparkConf)
    val textFile = sc.textFile("D:\\words.txt")
    val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    wordCount.foreach(println)
  }
}
