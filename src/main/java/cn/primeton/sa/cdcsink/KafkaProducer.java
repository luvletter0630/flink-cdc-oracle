package cn.primeton.sa.cdcsink;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.concurrent.ExecutionException;

@Slf4j
public class KafkaProducer {

    public static void sendMessage(String clusterIp, String topic, String key, String message) throws ExecutionException, InterruptedException {
        Producer<String, String> producer = ProducerSingleon.getProducer(clusterIp);
        // Send the message
        producer.send(new ProducerRecord<>(topic, key, message)).get();
    }
}
