package com.practice.kafka.event;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileEventHandler는 EventHandler 인터페이스를 구현하여
 * 수신된 메시지를 Kafka 토픽으로 전송하는 역할을 합니다.
 */
public class FileEventHandler implements EventHandler {

    public static final Logger logger = LoggerFactory.getLogger(FileEventHandler.class.getName());

    private KafkaProducer<String, String> kafkaProducer;
    private String topicName;
    private boolean sync;

    /**
     * FileEventHandler 생성자
     *
     * @param kafkaProducer KafkaProducer 인스턴스
     * @param topicName     메시지를 전송할 Kafka 토픽 이름
     * @param sync          동기 전송 여부
     */
    public FileEventHandler(KafkaProducer<String, String> kafkaProducer, String topicName, boolean sync) {
        this.kafkaProducer = kafkaProducer;
        this.topicName = topicName;
        this.sync = sync;
    }

    /**
     * 메시지를 Kafka 토픽으로 전송하는 메서드
     *
     * @param messageEvent 처리할 메시지 이벤트
     * @throws InterruptedException 비동기 작업 중 인터럽트 발생 시
     * @throws ExecutionException   비동기 작업 중 예외 발생 시
     */
    @Override
    public void onMessage(MessageEvent massageEvent) throws InterruptedException, ExecutionException {

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, massageEvent.key, massageEvent.value);

        if (this.sync) {
            // 동기 방식으로 메시지 전송
            RecordMetadata recordMetadata = this.kafkaProducer.send(producerRecord).get();

            logger.info("\n ##### record metadata received ##### \n" +
                    "partition: " + recordMetadata.partition() + "\n" +
                    "offset: " + recordMetadata.offset() + "\n" +
                    "timestamp: " + recordMetadata.timestamp());
        } else {
            // 비동기 방식으로 메시지 전송
            this.kafkaProducer.send(producerRecord, (recordMetadata, exception) -> {
                if (exception == null) {
                    logger.info("\n ##### record metadata received ##### \n" +
                            "partition: " + recordMetadata.partition() + "\n" +
                            "offset: " + recordMetadata.offset() + "\n" +
                            "timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("exception error from broker" + exception.getMessage());
                }
            });
        }

    }

    // FileEventHandler가 제대로 생성되었는지 확인을 위해 수행
    public static void main(String[] args) throws Exception {

        String topicName = "file-topic";

        Properties props  = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);
        boolean sync = true;

        FileEventHandler fileEventHandler = new FileEventHandler(kafkaProducer, topicName, sync);
        MessageEvent messageEvent = new MessageEvent("key00001", "this is test message");
        fileEventHandler.onMessage(messageEvent);
    }

}
