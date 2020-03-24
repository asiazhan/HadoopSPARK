package util

import java.io.{FileInputStream, InputStreamReader}
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.slf4j.LoggerFactory

import scala.collection.mutable

class CaseConf(propertiesPath: String) extends Serializable {

  val log = LoggerFactory.getLogger(getClass)
  private val settings = new mutable.HashMap[String, String]()
  load(propertiesPath)
  def load(propertiesPath: String): Unit ={
    loadPropertiesFiles(propertiesPath)
  }

  def loadPropertiesFiles(propertiesPath: String) ={
    var in: FSDataInputStream = null
    var inr: InputStreamReader = null
    try {
      val conf = new Configuration()
      val path = new Path(propertiesPath)
      val fs = path.getFileSystem(conf)

      in = fs.open(path)
      val properties = new Properties()

      inr = new InputStreamReader(in, "utf-8")
      properties.load(inr)

      val keys = properties.propertyNames()
      while (keys.hasMoreElements){
        val key = keys.nextElement().toString
        settings += ((key, properties.getProperty(key).trim))
      }
    }catch {
      case e:Exception =>{
        throw e
      }
    }finally {
      if(in != null){
        in.close()
      }
      if(inr != null){
        inr.close()
      }
    }
  }

  def loadLocal(propertiesPath: String) ={
    var in : FileInputStream = null
    try {
       in = new FileInputStream(propertiesPath)
      val properties = new Properties()
      properties.load(in)
      val keys = properties.propertyNames()
      while (keys.hasMoreElements){
        val key = keys.nextElement().toString
        settings += ((key, properties.getProperty(key).trim))
      }
    }catch {
      case e:Exception =>{
        throw e
      }
    }finally {
      if(in != null){
        in.close()
      }
    }
  }
  def get(key: String): String = {
    settings.getOrElse(key, throw new NoSuchElementException(key))
  }

  def get(key: String, value: String): String = {
    settings.getOrElse(key, value)
  }

  def getAll:Map[String, String] = {
    settings.toMap
  }

  def getOption(key: String): Option[String] = {
    settings.get(key)
  }
  def getInt(key: String, value: Int): Int = {
    getOption(key).map(_.toInt).getOrElse(value)
  }
  def getLong(key: String, value: Long): Long = {
    getOption(key).map(_.toLong).getOrElse(value)
  }

  def getDouble(key: String, value: Double): Double = {
    getOption(key).map(_.toDouble).getOrElse(value)
  }
}
