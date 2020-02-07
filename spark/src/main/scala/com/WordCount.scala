package com

import org.apache.derby.iapi.util.StringUtil
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2019/7/22.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordcounts").setMaster("local[*]")

    val sc = new SparkContext(conf)
//    sc.textFile("D:\\count.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).saveAsTextFile("D:\\spout.txt")
//    val rd1 = sc.textFile("D:\\count.txt",1).flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).saveAsTextFile("D:\\spout.txt")
    val pa=System.getProperty("user.dir");
    var path:String =null
    if(pa ==null || pa.size==0){
      path=args(0)
    }else{
      path="D:\\words.txt"
    }
    val rdd1 = sc.textFile(path);
    //压扁
    val rdd2 = rdd1.flatMap(line => line.split(" ")) ;
    //映射w => (w,1)
    val rdd3 = rdd2.map((_,1))
    val rdd4 = rdd3.reduceByKey(_ + _)
    val r = rdd4.collect()
    r.foreach(println)

  }
}
