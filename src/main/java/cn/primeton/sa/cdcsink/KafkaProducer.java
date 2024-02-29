package cn.primeton.sa.cdcsink;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;


public class KafkaProducer {


    public static void sendMessage(String clusterIp,String topic, String key, String message) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", clusterIp);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);

        // Send the message
        try {
            producer.send(new ProducerRecord<>(topic, key, message));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Close the producer
            producer.close();
        }
    }
}
