import com.util.HbaseUtil;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        HbaseUtil config = new HbaseUtil();
        config.createTable("scores",new String[]{"grade","score"});
    }
}
