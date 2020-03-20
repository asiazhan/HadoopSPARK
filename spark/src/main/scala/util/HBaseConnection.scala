package util


import com.google.common.collect.Table.Cell
import org.apache.hadoop.hbase.{CellUtil, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ArrayBuffer

class HBaseConnection(connection: Connection) extends HBaseCommands{
  @transient
  val conn: Connection = connection
  def close(): Unit = {
    if(conn != null && !conn.isClosed){
      conn.close()
    }
  }

  override def getAdmin(): Admin = {
    conn.getAdmin
  }

  /**
   *
   * @param tableName 表名
   * @param family 列族名称
   * @return
   */
  override def create(tableName: String, family: String*): Boolean = {
    var admin: Admin = null
    try {
      val htd = new HTableDescriptor(TableName.valueOf(tableName))
      for(i <- 0 until family.length){
        val cf= new HColumnDescriptor(family(i))
        htd.addFamily(cf)
      }
      admin = getAdmin
      val tableExists = admin.tableExists(TableName.valueOf(tableName))
      if(tableExists){
        return false
      }
      admin.createTable(htd)
      return true
    }catch {
      case e:Exception =>{
        throw e
      }
    }finally {
      if(admin !=null){
        admin.close()
      }
    }
    false
  }

  /**
   *
   * @param tableName 表名
   * @param htds   表描述
   * @return
   */
  override def create(tableName: String, htds: HTableDescriptor): Boolean = {
    var admin: Admin = null
    try {
      admin = getAdmin
      val tableExists = admin.tableExists(TableName.valueOf(tableName))
      if(tableExists){
        return false
      }
      admin.createTable(htds)
      return true
    }catch {
      case e:Exception =>{
        throw e
      }
    }finally {
      if(admin !=null){
        admin.close()
      }
    }
     false
  }

  override def addFamily(tableName: String, family: String*): Unit = {
    var admin: Admin = null
    try {
      admin = getAdmin
      val table = TableName.valueOf(tableName)
      admin.disableTable(table)
      val htd = admin.getTableDescriptor(TableName.valueOf(tableName))
      for(item <- family){
        htd.addFamily(new HColumnDescriptor(Bytes.toBytes(item)))
      }
      admin.modifyTable(htd)
      admin.enableTable(table)
    }catch {
      case e:Exception =>{
        throw e
      }
    }finally {
      if(admin !=null){
        admin.close()
      }
    }
  }

  override def deleteTable(tableName: String): Unit = {
    var admin: Admin = null
    try {
      admin = getAdmin
      val table = TableName.valueOf(tableName)
      admin.disableTable(table)
      admin.deleteTable(table)
    }catch {
      case e:Exception =>{
        throw e
      }
    }finally {
      if(admin !=null){
        admin.close()
      }
    }
  }

  override def put(tableName: String, rowkey: String, familyName: String, column: String, value: String): Boolean = {
    var table: Table = null
    try {
      val put = new Put(Bytes.toBytes(rowkey))
      put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column), Bytes.toBytes(value))
      val table = conn.getTable(TableName.valueOf(tableName))
      table.put(put)
      return true
    }catch {
      case e:Exception =>{
        throw e
      }
    }finally {
      if(table !=null){
        table.close()
      }
    }
    false
  }

  override def get(tableName: String, rowkey: String): Result = {
    var table: Table = null
    try {
      val table = conn.getTable(TableName.valueOf(tableName))
      val get = new Get(Bytes.toBytes(rowkey))
      return table.get(get)
    }catch {
      case e:Exception =>{
        throw e
      }
    }finally {
      if(table !=null){
        table.close()
      }
    }
    null
  }

  override def get(tableName: String, rowkey: String, familyName: String, column: String): Result = {
    var table: Table = null
    try {
       table = conn.getTable(TableName.valueOf(tableName))
      val get = new Get(Bytes.toBytes(rowkey))
      get.addColumn(Bytes.toBytes(familyName),Bytes.toBytes(column))
      return table.get(get)
    }catch {
      case e:Exception =>{
        throw e
      }
    }finally {
      if(table !=null){
        table.close()
      }
    }
    null
  }

  override def getValue(tableName: String, rowkey: String, familyName: String, column: String): String = {
    var table: Table = null
    try {
       table = conn.getTable(TableName.valueOf(tableName))
      val get = new Get(Bytes.toBytes(rowkey))
      get.addColumn(Bytes.toBytes(familyName),Bytes.toBytes(column))
      val result =  table.get(get)
      val value = result.getValue(Bytes.toBytes(familyName),Bytes.toBytes(column))
      return Bytes.toString(value)
    }catch {
      case e:Exception =>{
        throw e
      }
    }finally {
      if(table !=null){
        table.close()
      }
    }
    null
  }

  override def getSanner(tableName: String, scan: Scan): ResultScanner = {
    var table: Table = null
    try {
       table = conn.getTable(TableName.valueOf(tableName))
      return table.getScanner(scan)
    }catch {
      case e:Exception =>{
        throw e
      }
    }finally {
      if(table !=null){
        table.close()
      }
    }
    null
  }

  override def getColumnsLatestValue(tableName: String, rowkey: String): collection.mutable.HashMap[String, String] = {
    var table: Table = null
    val map = new scala.collection.mutable.HashMap[String, String]()
    try {
      table = conn.getTable(TableName.valueOf(tableName))
      val get = new Get(Bytes.toBytes(rowkey))
      val result =  table.get(get)
      if(result.rawCells() == null){
        return map
      }
      for(cel <- result.rawCells()){
        map.put(Bytes.toString(CellUtil.cloneQualifier(cel)),Bytes.toString(CellUtil.cloneValue(cel)))
      }
      return map
    }catch {
      case e:Exception =>{
        throw e
      }
    }finally {
      if(table !=null){
        table.close()
      }
    }
    map
  }

  override def deleteRow(tableName: String, rowkey: String*): Unit = {
    var table: Table = null
    try {
      table = conn.getTable(TableName.valueOf(tableName))
      val list  = new ArrayBuffer[Delete]()

      for(row <- rowkey){
        val del: Delete = new Delete(Bytes.toBytes(row))
        list += del
      }
      table.delete(list.asInstanceOf[Delete])
      table.close()
    }catch {
      case e:Exception =>{
        throw e
      }
    }finally {
      if(table !=null){
        table.close()
      }
    }
  }
  def deleteRow(tableName: String, rowkey: String): Unit = {
    var table: Table = null
    try {
      table = conn.getTable(TableName.valueOf(tableName))
      val del: Delete = new Delete(Bytes.toBytes(rowkey))
      table.delete(del)
      table.close()
    }catch {
      case e:Exception =>{
        throw e
      }
    }finally {
      if(table !=null){
        table.close()
      }
    }
  }
  override def disableTable(tableName: String): Unit = {
    var admin: Admin = null
    try {
      admin = getAdmin
      val table = TableName.valueOf(tableName)
      admin.disableTable(table)
    }catch {
      case e:Exception =>{
        throw e
      }
    }finally {
      if(admin !=null){
        admin.close()
      }
    }
  }

  override def delete(tableName: String, rowkey: String, familyName: String, column: String): Unit = {
    var table: Table = null
    try {
      val del = new Delete(Bytes.toBytes(rowkey))
      del.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column))
      table = conn.getTable(TableName.valueOf(tableName))
      return table.delete(del)
    }catch {
      case e:Exception =>{
        throw e
      }
    }finally {
      if(table !=null){
        table.close()
      }
    }
  }

  override def exists(tableName: String, rowkey: String): Boolean = {
    var table: Table = null
    try {
      val get = new Get(Bytes.toBytes(rowkey))
      table = conn.getTable(TableName.valueOf(tableName))
      return table.exists(get)
    }catch {
      case e:Exception =>{
        throw e
      }
    }finally {
      if(table !=null){
        table.close()
      }
    }
    false
  }

  override def tableExists(table: String): Boolean = {
    var admin: Admin = null
    try {
      admin = getAdmin
      return admin.tableExists(TableName.valueOf(table))
    }catch {
      case e:Exception =>{
        throw e
      }
    }finally {
      if(admin !=null){
        admin.close()
      }
    }
    false
  }

  override def getTable(tableName: String): Table = {
    return conn.getTable(TableName.valueOf(tableName))
  }
}
