package com.example.kafka;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleConsumer {

    public static final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class.getName());

    public static void main(String[] args) {

        String topicName = "simple-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group1_01");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
        // 지정된 토픽을 구독
        kafkaConsumer.subscribe(List.of(topicName));

        // 무한 루프를 돌며 메시지를 지속적으로 폴링
        while (true) {
            ConsumerRecords<String, String> consumerRecords= kafkaConsumer.poll(Duration.ofMillis(1000));

            // 폴링된 각 메시지를 순회하며 처리
            for (ConsumerRecord record : consumerRecords) {
                    logger.info("record key: {}, record value: {}, partition: {}", record.key(), record.value(), record.partition());
            }
        }
    }
}
