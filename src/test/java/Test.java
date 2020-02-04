
import com.util.HbaseUtil;
import com.util.KaConsumer;
import com.util.KaProducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;


/**
 * Created by Administrator on 2019/6/18.
 */
public class Test
{

    @org.junit.Test
    public void testCreateTable() throws Exception {
        //创建hbase的配置对象
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node2,node3,node4");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        //创建hbase的连接对象
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        //创建一个表定义描述对象
        HTableDescriptor tUser = new HTableDescriptor(TableName.valueOf("t_user"));
        //构造一个列族描述对象
        HColumnDescriptor f1 = new HColumnDescriptor("f1");
        HColumnDescriptor f2 = new HColumnDescriptor("f2");
        //在表描述对象中加入列族描述
        tUser.addFamily(f1);
        tUser.addFamily(f2);
        //调用admin的建表方法来建表
        admin.createTable(tUser);
        System.out.println("success");
// 关闭连接
        admin.close();
        conn.close();
    }
    HbaseUtil hBaseClient = new HbaseUtil();
    /**
     * 测试删除、创建表
     */
    @org.junit.Test
    public void testGetValue() {
        String value = hBaseClient.getValue("tbl_abc", "rowKey1", "info", "age");
        System.out.println(value);
    }

    @org.junit.Test
    public void CreateTable() throws IOException {
        String tableName = "tbl_abc";
        hBaseClient.deleteTable(tableName);
        hBaseClient.createTable(tableName, new String[] {"cf1", "cf2"});
    }

    @org.junit.Test
    public void dropTable() throws IOException {
        hBaseClient.deleteTable("tbl_abc");
    }


    @org.junit.Test
    public void testInsertOrUpdate() throws IOException {
        hBaseClient.insertOrUpdate("tbl_abc", "rowKey1", "cf1", new String[]{"c1", "c2"}, new String[]{"v1", "v22"});
    }


    @org.junit.Test
    public void testScanTable() throws IOException {
        hBaseClient.scanTable("tbl_abc", "rowKey1");
    }
    @org.junit.Test
    public void testgGetRow() throws IOException {
        hBaseClient.getRow("tbl_abc", "rowKey1");
    }

    @org.junit.Test
    public void testGetRowQualifier() throws IOException {
        hBaseClient.getRowQualifier("tbl_abc", "rowKey1","cf1","c1");
    }
    KaProducer producer =new KaProducer();
    @org.junit.Test
    public void testkafkaSend() {
        String name = "first";
        String val = "test--";
        for (int i = 0; i < 50; i++) {
          producer.send(name,val+i);

        }
    }

    KaConsumer cos=new KaConsumer();
    @org.junit.Test
    public void testKaConsumer() throws IOException {
        String name = "first";
        cos.kafkaGet(name);
    }
}
