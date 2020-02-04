package com.util;

import io.netty.util.internal.StringUtil;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PropertiesUtil {
    private static Map<String, Properties> loaderMap = new HashMap<String, Properties>();
    private static ConcurrentMap<String, String> configMap = new ConcurrentHashMap<String, String>();
    private static  String DEFAULT_CONFIG_FILE = null;
    private  static String pa=System.getProperty("user.dir");
    private static Properties prop = null;
    static{
        Properties prop = new Properties();
            try{
            //读取属性文件a.properties
            String pa=System.getProperty("user.dir");
            String path = pa+ "\\src\\Resources\\"+"kafka.properties";
            InputStream in = new BufferedInputStream(new FileInputStream(path));
            prop.load(in);     ///加载属性列表
            Iterator<String> it=prop.stringPropertyNames().iterator();
            while(it.hasNext()){
                String key=it.next();
                if (!configMap.containsKey(key)) {
                    if (prop.getProperty(key) != null) {
                        configMap.put(key, prop.getProperty(key));
                    }
                }
                System.out.println(key+":"+prop.getProperty(key));
            }
            in.close();

        }
            catch(Exception e){
            System.out.println(e);
        }
    }



    public static String getStringByKey(String key) {
        return configMap.get(key);
    }

    public static Properties getProperties() {
        return prop;
    }


}
