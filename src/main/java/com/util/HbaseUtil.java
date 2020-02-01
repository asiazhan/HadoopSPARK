package com.util;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseUtil {
    private static Connection conn;
    private static Admin admin;
    static {
        Configuration conf = HBaseConfiguration.create();
        // 与hbase/conf/hbase-site.xml 中 hbase.zookeeper.quorum 配置的值相同
        conf.set("hbase.zookeeper.quorum", "node2,node3,node4");
        // 与hbase/conf/hbase-site.xml 中 hbase.zookeeper.property.clientPort 配置的值相同
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void createTable(String myTableName,String[] colFamily) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        if(admin.tableExists(tableName)){
            System.out.println("table exists!");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for(String str:colFamily){
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }
    }

    public  void insertData(String tableName, String rowKey, String colFamily, String col, String val) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(),col.getBytes(),val.getBytes());
        table.put(put);
        table.close();
        System.out.println("插入数据成功");
    }
    public static void addRowData(String tableName, String rowKey, String colFamily, String
            col, String val) throws IOException{
        //创建HTable对象
        Table table = conn.getTable(TableName.valueOf(tableName));
        //向表中插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        /* 向Put对象中组装数据 */
        put.addColumn(colFamily.getBytes(),col.getBytes(),val.getBytes());
        table.put(put);
        table.close();
        System.out.println("插入数据成功");
    }
//    public static void dropTable(String tableName) throws MasterNotRunningException,
//            ZooKeeperConnectionException, IOException{
//        if(isTableExist(tableName)){
//            admin.disableTable(tableName);
//            admin.deleteTable(tableName);
//            System.out.println("表" + tableName + "删除成功！");
//        }else{
//            System.out.println("表" + tableName + "不存在！");
//        }
//    }


}
