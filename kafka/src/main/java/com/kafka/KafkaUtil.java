package com.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;


import java.io.Serializable;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaUtil implements Serializable {
    public static Log log = LogFactory.getLog(KafkaUtil.class);

    private static Producer<String, String> producer = null;

    private static KafkaProducer<String, String> kafkaProducer = null;
    public synchronized static Producer<String, String> getProducer(String outBrokers){
        if(null == producer){
            Properties props = new Properties();
            props.put("metadata.broker.list",outBrokers);
            props.put("serializer.class","kafka.serializer.StringEncoder");
            props.put("request.required.acks","-1");
            ProducerConfig config = new ProducerConfig(props);
            producer = new Producer<String, String>(config);
        }
        return producer;
    }

    public synchronized static KafkaProducer<String, String> getAuthProducer(String outBrokers, String username, String password){
        if(null == kafkaProducer){
            Properties props = new Properties();
            props.put("bootstrap.servers",outBrokers);
            props.put("key.serializer", StringSerializer.class);
            props.put("value.serializer", StringSerializer.class);
            props.put("request.required.acks","-1");
            props.put("security.protocol","");
            props.put("sasl.mechanism","");
            String jaas = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{0}\" password=\"{1}\";";
            props.put("sasl.jass.config", MessageFormat.format(jaas, username, password));
            ProducerConfig config = new ProducerConfig(props);
            kafkaProducer = new KafkaProducer<String, String>(props);
        }
        return kafkaProducer;
    }

    /**
     * 不带验证的
     * @param outBrokers
     * @param topic
     * @param msg
     */
    public static void sendMsgToKafka(String outBrokers, String topic, String[] msg){
        long start = System.currentTimeMillis();
        Producer<String, String> producer=getProducer(outBrokers);
        List<KeyedMessage<String, String>> msgList =new ArrayList<KeyedMessage<String, String>>();
        for (String ms: msg) {
            String key = Math.round(Math.random() * 100000) + "";
            msgList.add(new KeyedMessage<String, String>(topic, key, ms));
            log.debug("发送消息："+ ms);
        }
        producer.send(msgList);
        long end = System.currentTimeMillis();
        log.debug("Seng "+msg.length + "msg to kafka total cost:" + (end-start) + "ms");
    }

}
