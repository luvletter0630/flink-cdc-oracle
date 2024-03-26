package cn.primeton.sa.cdcsink;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class KafkaProducer {

    public static void sendMessage(String clusterIp, String topic, String key, String message) throws ExecutionException, InterruptedException {
        Producer<String, String> producer = ProducerSingleon.getProducer(clusterIp, topic, key, message);
        // Send the message
        producer.send(new ProducerRecord<>(topic, key, message)).get();
    }
}
