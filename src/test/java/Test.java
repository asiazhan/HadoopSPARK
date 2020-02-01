
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;




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
}
