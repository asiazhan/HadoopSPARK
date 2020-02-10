package com.kafka;


import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import java.util.concurrent.Future;

public class KaProducer {
    static Properties props=new Properties();
    static{
//        props.put("bootstrap.servers", "master:9092");//kafka集群，broker-list
        props.put("bootstrap.servers", "master:9092,node1:9092,node2:9092,node3:9092");//kafka集群，broker-list
        props.put("acks", "all");
        props.put("retries", 1);//重试次数
        props.put("batch.size", 16384);//批次大小
        props.put("linger.ms", 1);//等待时间
        props.put("buffer.memory", 33554432);//RecordAccumulator缓冲区大小
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      }
        public void send(String topic, String st){
          Producer<String, String> producer = new KafkaProducer(props);
          producer.send(new ProducerRecord<String, String>(topic, st));
        }

    public void sendBac(String topic, String st){
        Producer<String, String> producer = new KafkaProducer(props);
        for (int i = 0; i < 100; i++) {
            Future<RecordMetadata> first = producer.send(new ProducerRecord<String, String>("first", Integer.toString(i), Integer.toString(i)), new Callback() {

                //回调函数，该方法会在Producer收到ack时调用，为异步调用
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println("success->" + metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                }
            });
        }
        producer.close();

    }


}

