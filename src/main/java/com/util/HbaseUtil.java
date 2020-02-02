package com.util;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;



public class HbaseUtil {
    private static Connection conn;
    private static Admin admin;
    static {
        Configuration conf = HBaseConfiguration.create();
        // 与hbase/conf/hbase-site.xml 中 hbase.zookeeper.quorum 配置的值相同
        conf.set("hbase.zookeeper.quorum", "master,node1,node2");
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

    public boolean tableExists(String tableName) throws IOException {
        TableName[] tableNames = admin.listTableNames();
        if (tableNames != null && tableNames.length > 0) {
            for (int i = 0; i < tableNames.length; i++) {
                if (tableName.equals(tableNames[i].getNameAsString())) {
                    return true;
                }
            }
        }
        return false;
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
    public void descTable(String tableName) throws Exception {
        Table table = conn.getTable(TableName.valueOf(tableName));
        TableDescriptor descriptor = table.getDescriptor();
        ColumnFamilyDescriptor[] columnFamilies = descriptor.getColumnFamilies();
        for(ColumnFamilyDescriptor t:columnFamilies){
            System.out.println(Bytes.toString(t.getName()));
        }
    }

    public void deleteRow1(String tableName, String rowKey) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowKey.getBytes());
        table.delete(delete);
    }


    public void createTable1(String tableName, String[] columnFamilies) throws IOException {
        TableName name = TableName.valueOf(tableName);

        boolean isExists = this.tableExists(tableName);
        if (isExists) {
            throw new TableExistsException(tableName + "is exists!");
        }

        TableDescriptorBuilder descriptorBuilder = TableDescriptorBuilder.newBuilder(name);
        List<ColumnFamilyDescriptor> columnFamilyList = new ArrayList<ColumnFamilyDescriptor>();
        for (String columnFamily : columnFamilies) {
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(columnFamily.getBytes()).build();
            columnFamilyList.add(columnFamilyDescriptor);
        }
        descriptorBuilder.setColumnFamilies(columnFamilyList);
        TableDescriptor tableDescriptor = descriptorBuilder.build();
        admin.createTable(tableDescriptor);
    }
    public void insertOrUpdate(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
        this.insertOrUpdate(tableName, rowKey, columnFamily, new String[]{column}, new String[]{value});
    }

    public void insertOrUpdate(String tableName, String rowKey, String columnFamily, String[] columns, String[] values) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        for (int i = 0; i < columns.length; i++) {
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columns[i]), Bytes.toBytes(values[i]));
            table.put(put);
        }
    }

    public void deleteRow(String tableName, String rowKey) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowKey.getBytes());
        table.delete(delete);
    }

    public void deleteColumnFamily(String tableName, String rowKey, String columnFamily) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowKey.getBytes());
        delete.addFamily(Bytes.toBytes(columnFamily));
        table.delete(delete);
    }

    public void deleteColumn(String tableName, String rowKey, String columnFamily, String column) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowKey.getBytes());
        delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        table.delete(delete);
    }

    public void deleteTable(String tableName) throws IOException {
        boolean isExists = this.tableExists(tableName);
        if (!isExists) {
            return;
        }

        TableName name = TableName.valueOf(tableName);
        admin.disableTable(name);
        admin.deleteTable(name);
    }
    public String getValue(String tableName, String rowkey, String family, String column) {
        Table table = null;

        String value = "";
        if (StringUtils.isBlank(tableName) || StringUtils.isBlank(family)
                || StringUtils.isBlank(rowkey) || StringUtils.isBlank(column)) {
            return null;
        }

        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Get g = new Get(rowkey.getBytes());
            g.addColumn(family.getBytes(), column.getBytes());
            Result result = table.get(g);
            List<Cell> ceList = result.listCells();
            if (ceList != null && ceList.size() > 0) {
                for (Cell cell : ceList) {
                    value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                table.close();
                conn.close();
            }catch (IOException e) {
                e.printStackTrace();
            }
        }
        return value;
    }

    public String scanTable(String tableName, String rowKeyFilter) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        if (!StringUtils.isEmpty(rowKeyFilter)) {
            RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL, new SubstringComparator(rowKeyFilter));
            scan.setFilter(rowFilter);
        }
        ResultScanner scanner = table.getScanner(scan);

        try {
            for (Result result : scanner) {
                System.out.println(Bytes.toString(result.getRow()));
                for (Cell cell : result.rawCells()) {
                    System.out.println(cell);
                }
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }

        return null;
    }

    /**
     *  获取某一行数据
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void getRow(String tableName, String rowKey) throws IOException{

        Table table = conn.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));

        //get.setMaxVersions();显示所有版本

        //get.setTimeStamp();显示指定时间戳的版本

        Result result = table.get(get);

        for(Cell cell : result.rawCells()){

            System.out.println("行键:" + Bytes.toString(result.getRow()));

            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));

            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));

            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));

            System.out.println("时间戳:" + cell.getTimestamp());

        }

    }

    /**
     * 获取某一行指定“列族:列”的数据
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @throws IOException
     */
    public static void getRowQualifier(String tableName, String rowKey, String family, String

            qualifier) throws IOException{

        Table table = conn.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));

        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));

        Result result = table.get(get);

        for(Cell cell : result.rawCells()){

            System.out.println("行键:" + Bytes.toString(result.getRow()));

            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));

            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));

            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));

        }

    }
}
