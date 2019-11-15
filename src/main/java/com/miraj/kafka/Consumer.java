package com.miraj.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Consumer {

	
    public static void main(String[] args) {
    
    	KafkaConsumer<String, String> consumer = null;
//    	String keytabLocation = "/etc/security/keytabs/cdap.headless.keytab";
//    	String principal = "cdap-raf010-reflex-platform@GVS.GGN";

    	 String keytabLocation = "/etc/security/keytabs/cdap.headless.keytab";
     	String principal = "cdap-rafa001-reflex-platform@GVS.GGN";
     	
    	Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , "raf010-slv-04.cloud.in.guavus.com:6667");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_topic1");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");
        props.put(SaslConfigs.SASL_MECHANISM, "GSSAPI");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        props.put(SaslConfigs.SASL_JAAS_CONFIG, String.format("com.sun.security.auth.module.Krb5LoginModule required \n" +
                "        useKeyTab=true \n" +
                "        storeKey=true  \n" +
                "        useTicketCache=false  \n" +
                "        renewTicket=true  \n" +
                "        keyTab=\"%s\" \n" +
                "        principal=\"%s\";",
				keytabLocation, principal));
        
		// Checks connection by extracting topics.
		try {
			consumer = new KafkaConsumer<>(props);
        	System.out.println("Number of topis are: " + consumer.listTopics().size());
//        	consumer.listTopics().forEach((k,v) -> System.out.println("keys" + k + "Values"+ v));

//        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
			List<String> topics = new ArrayList<String>();
			topics.add("test_topic");
			consumer.subscribe(topics);
			int i = 0;
			while (i++ < 10) {
				System.out.println("going to print : records");
				ConsumerRecords<String, String> records = consumer.poll(10);
				for (ConsumerRecord<String, String> record : records) {
					System.out.println(String.format("Topic - %s, Partition - %d, Value: %s, Offset: %s, Key: %s", record.topic(),
							record.partition(), record.value(), record.offset(), record.key()));
				}
			}

		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} finally {
			consumer.close();
		}
    }
}
