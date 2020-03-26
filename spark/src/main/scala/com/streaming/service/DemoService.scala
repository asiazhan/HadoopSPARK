package com.streaming.service

import com.streaming.engine.DemoEngine
import util.CaseConf

object DemoService {

  def main(args: Array[String]): Unit = {
    val dir = System.getProperty("user.dir")
    val arr = Array("Demo.properties")
        if(dir.contains("HadoopAPARK")){
          new DemoEngine().run(arr, dir)
        }else{
          new DemoEngine().run(args, dir )
        }
    println(com.streaming.service.DemoService)
  }



}
