package com.miraj.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Consumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "raf010-slv-04.cloud.in.guavus.com:9092,raf010-slv-05.cloud.in.guavus.com:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "test-group");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.kerberos.service.name", "kafka");

        // Checks connection by extracting topics.
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
          consumer.listTopics();
        
//        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
//        List<String> topics = new ArrayList<String>();
//        topics.add("devglan-partitions-topic");
//        kafkaConsumer.subscribe(topics);
//        try{
//            while (true){
//                ConsumerRecords<String, String> records = kafkaConsumer.poll(10);
//                for (ConsumerRecord<String, String> record: records){
//                    System.out.println(String.format("Topic - %s, Partition - %d, Value: %s", record.topic(), record.partition(), record.value()));
//                }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }
}
