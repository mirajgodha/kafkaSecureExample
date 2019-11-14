package com.miraj.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Consumer {

	public static final String SASL_JAAS_CONFIG = "sasl.jaas.config";
	
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "raf010-slv-04.cloud.in.guavus.com:6667");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "test_topic");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.kerberos.service.name", "Kafka");
        props.put("sasl.mechanism", "GSSAPI");
        String keytabLocation = "/etc/security/keytabs/cdap.headless.keytab";
        String principal = "cdap-raf010-reflex-platform@GVS.GGN";
        
        props.put(SASL_JAAS_CONFIG, String.format("com.sun.security.auth.module.Krb5LoginModule required \n" +
                "        useKeyTab=true \n" +
                "        storeKey=true  \n" +
                "        useTicketCache=false  \n" +
                "        renewTicket=true  \n" +
                "        keyTab=\"%s\" \n" +
                "        principal=\"%s\";",
        keytabLocation, principal));
        KafkaConsumer<String, String> consumer=null;
        // Checks connection by extracting topics.
        try  {
        	consumer = new KafkaConsumer<>(props);
//        	System.out.println("Number of topis are: " + consumer.listTopics().size());
//        	consumer.listTopics().forEach((k,v) -> System.out.println("keys" + k + "Values"+ v));
        	
        	
//        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
        List<String> topics = new ArrayList<String>();
        topics.add("test_topic");
        consumer.subscribe(topics);
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(10);
                for (ConsumerRecord<String, String> record: records){
                    System.out.println(String.format("Topic - %s, Partition - %d, Value: %s", record.topic(), record.partition(), record.value()));
                }
            }
        	
        }catch (Exception e){
            System.out.println(e.getMessage());
            e.printStackTrace();
        }finally {
        	consumer.close();
        }
    }
}
