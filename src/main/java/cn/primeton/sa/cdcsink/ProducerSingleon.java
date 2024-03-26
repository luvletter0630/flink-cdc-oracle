package cn.primeton.sa.cdcsink;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;

/**
 * @author liwj
 * @date 2024/3/26 12:13
 * @description:
 */
public class ProducerSingleon {
    private static Producer producer;

    private ProducerSingleon() {
    }

    public static Producer getProducer(String clusterIp) {
        if (producer == null) {
            synchronized (ProducerSingleon.class) {
                if (producer == null) {
                    producer = initProducer(clusterIp);
                }
            }
        }
        return producer;
    }

    private static Producer initProducer(String clusterIp) {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterIp);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"admin-succ\";");
        props.put("acks", "all");
        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
        return producer;
    }
}
