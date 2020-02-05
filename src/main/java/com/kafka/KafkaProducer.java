//package com.kafka;
//
//import org.codehaus.janino.util.Producer;
//
//import java.util.Properties;
//
//public class KafkaProducer extends Thread{
//    private String topic;
//
//    private Producer<Integer, String> producer;
//
//    public KafkaProducer(String topic) {
//        this.topic = topic;
//
//        Properties properties = new Properties();
//
//        properties.put("metadata.broker.list",KafkaProperties.BROKER_LIST);
//        properties.put("serializer.class","kafka.serializer.StringEncoder");
//        properties.put("request.required.acks","1");
//
//        producer = new Producer<Integer, String>(new ProducerConfig(properties));
//    }
//
//    @Override
//    public void run() {
//
//        int messageNo = 1;
//
//        // 循环产生数据流
//        while(true) {
//            String message = "fyy_message_" + messageNo;
//            producer.send(new KeyedMessage<Integer, String>(topic, message));
//            System.out.println("Sent: " + message);
//
//            messageNo ++ ;
//
//            try{
//                Thread.sleep(1000);
//            } catch (Exception e){
//                e.printStackTrace();
//            }
//        }
//
//    }
//
//
//}