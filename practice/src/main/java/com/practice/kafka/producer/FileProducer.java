package com.practice.kafka.producer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileProducer {

    public static final Logger logger = LoggerFactory.getLogger(FileProducer.class.getName());

    public static void main(String[] args) {

        String topicName = "file-topic";
        String filePath = "C:\\Users\\Junsik\\workspace\\KafkaProj-01\\practice\\src\\main\\resources\\pizza_sample.txt";

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.56.101:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        // KafkaProducer 생성 => ProducerRecords 생성 => send() 비동기 방식 전송
        sendFileMessages(kafkaProducer, topicName, filePath);
        
        kafkaProducer.close();
    }

    /**
     * 지정된 파일을 읽어 각 라인을 Kafka 토픽으로 전송하는 메서드
     *
     * @param kafkaProducer KafkaProducer 인스턴스
     * @param topicName     메시지를 전송할 Kafka 토픽 이름
     * @param filePath      읽어올 파일의 경로
     */
    private static void sendFileMessages(KafkaProducer<String, String> kafkaProducer, String topicName, String filePath) {

        String line = "";
        final String delimiter = ",";

        try {
            FileReader fileReader = new FileReader(filePath);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while ((line = bufferedReader.readLine()) != null) {
                String[] tokens = line.split(delimiter);
                String key = tokens[0];
                StringBuffer value = new StringBuffer();

                for (int i = 1; i < tokens.length; i++) {
                    if (i != tokens.length - 1) {
                        value.append(tokens[i] + delimiter);
                    } else {
                        value.append(tokens[i]);
                    }
                }

                // Kafka 토픽으로 메시지 전송
                sendMessage(kafkaProducer, topicName, key, value.toString());
            }

        } catch (IOException e) {
            logger.info(e.getMessage());
        }
    }

    /**
     * Kafka 토픽으로 단일 메시지를 전송하는 메서드
     *
     * @param kafkaProducer KafkaProducer 인스턴스
     * @param topicName     메시지를 전송할 Kafka 토픽 이름
     * @param key           메시지 키
     * @param value         메시지 값
     */
    private static void sendMessage(KafkaProducer<String, String> kafkaProducer, String topicName, String key, String value) {

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, value);
        logger.info("key: {}, value: {}", key, value);

        // 메시지를 비동기 방식으로 Kafka에 전송
        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
                if (exception == null) {
                    logger.info("\n ##### record metadata received ##### \n" +
                            "partition: " + recordMetadata.partition() + "\n" +
                            "offset: " + recordMetadata.offset() + "\n" +
                            "timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("exception error from broker" + exception.getMessage());
                }
            }
        });
    };
}
