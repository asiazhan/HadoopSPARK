package com.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Demo1 {
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("Hehho").setMaster("local[*]")

      val sc = new SparkContext(conf)
      val path = use.
      //    val spark = new SparkSession(sc)
      val spark = SparkSession.builder().config(conf).getOrCreate()
      val df = spark.read.json("/usr/local/spark2.4/work/test.json")
      df.show()
      df.select("name").show()
      df.select("name", "age").show()
      df.createTempView("people")
      spark.sql("select * from people where age='21'").show()
      spark.close()
      sc.stop()
    }
}
