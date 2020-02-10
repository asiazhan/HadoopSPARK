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
        props.put("bootstrap.servers", "master:9092");
        props.put("group.id", "first");//指定消费者属于哪个组
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    }
        public  void kafkaGet(String topic){
            props.put("enable.auto.commit", "true");//开启kafka的offset自动提交功能，可以保证消费者数据不丢失
            KafkaConsumer<String, String> consumer = new KafkaConsumer(props);
            consumer.subscribe(Arrays.asList(topic));//指定消费的topic
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records)
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
        //KafkaConsumer：需要创建一个消费者对象，用来消费数据
        //ConsumerConfig：获取所需的一系列配置参数
        //ConsuemrRecord：每条数据都要封装成一个ConsumerRecord对象
    public  void kafkaGetshoudongOffsetTong(String topic){
            props.put("enable.auto.commit", "false");//自动提交offset
            KafkaConsumer<String, String> consumer = new KafkaConsumer(props);
            consumer.subscribe(Arrays.asList("first"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                }
                consumer.commitSync();
            }

    }
    public void kafkaGetshoudongOffsetYiBU(String topic){
        props.put("enable.auto.commit", "false");//自动提交offset
        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList("first"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
            consumer.commitAsync();
        }

    }

}

