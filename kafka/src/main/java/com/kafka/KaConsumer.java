package com.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;

public class KaConsumer {
    static Properties props=new Properties();
    static{
        props.put("bootstrap.servers", "node1:9092");
        props.put("group.id", "first");//指定消费者属于哪个组
        props.put("enable.auto.commit", "true");//开启kafka的offset自动提交功能，可以保证消费者数据不丢失
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    }
        public void kafkaGet(String topic){
            KafkaConsumer<String, String> consumer = new KafkaConsumer(props);
            consumer.subscribe(Arrays.asList(topic));//指定消费的topic
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records)
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }


}

