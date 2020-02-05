import com.kafka.PropertiesUtil;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
//        HbaseUtil config = new HbaseUtil();
//        config.createTable("scores",new String[]{"grade","score"});
        System.out.println(PropertiesUtil.getStringByKey("INTOPIC"));
//        System.out.println(System.getProperty("user.dir"));
    }
}
