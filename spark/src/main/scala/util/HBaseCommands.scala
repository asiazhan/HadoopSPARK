package util

import org.apache.hadoop.hbase.{HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._


trait HBaseCommands {
   //获取admin
  def getAdmin(): Admin
  //建表
  def create(tableName: String, family: String*): Boolean
  //建表
  def create(tableName: String, htds: HTableDescriptor): Boolean
  //增加列族
  def addFamily(tableName: String, family: String*): Unit
  //删表
  def deleteTable(tableName: String): Unit
  //更新、插入数据
  def put(tableName: String, rowkey: String, familyName: String, column: String, value: String): Boolean
  //根据rowkey查询
  def get(tableName: String, rowkey: String): Result
  //查询指定列族
  def get(tableName: String, rowkey: String, familyName: String, column: String): Result
  //获取某列最新之
  def getValue(tableName: String, rowkey: String, familyName: String, column: String): String
  //遍历查询表
  def getSanner(tableName: String, scan: Scan): ResultScanner
  //根据rowkey查询所有列族，列族数据置于map中
  def getColumnsLatestValue(tableName: String, rowkey: String): collection.mutable.HashMap[String, String]
  //删除指定rowkey
  def deleteRow(tableName: String, rowkey: String*): Unit
  //让表失效
  def disableTable(tableName: String): Unit
  //删除指定列
  def delete(tableName: String, rowkey: String, familyName: String, column: String)
  //判断指定rowkey是否在table中
  def exists(tableName: String, rowkey: String): Boolean
  //表是否存在
  def tableExists(table: String): Boolean
  //获取表对象，使用完毕调用Table.close关闭
  def getTable(tableName: String): Table


}
