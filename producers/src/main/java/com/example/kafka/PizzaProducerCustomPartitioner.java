package com.example.kafka;

import com.github.javafaker.Faker;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PizzaProducerCustomPartitioner는 Apache Kafka를 사용하여
 * 가상의 피자 메시지를 생성하고, 커스텀 파티셔너를 통해 특정 기준에 따라
 * 메시지를 전송하는 역할을 합니다.
 */
public class PizzaProducerCustomPartitioner {

    // SLF4J 로거 초기화
    public static final Logger logger = LoggerFactory.getLogger(PizzaProducerCustomPartitioner.class.getName());

    /**
     * 주어진 설정에 따라 피자 메시지를 생성하고 Kafka에 전송합니다.
     *
     * @param kafkaProducer         KafkaProducer 인스턴스
     * @param topicName             메시지를 전송할 Kafka 토픽 이름
     * @param iterCount             총 전송할 메시지 수 (-1은 무한 반복)
     * @param interIntervalMillis   각 메시지 전송 간의 간격 시간 (밀리초)
     * @param intervalMillis        특정 간격마다 추가로 대기할 시간 (밀리초)
     * @param intervalCount         몇 개의 메시지 전송 후 추가 대기할지 결정
     * @param sync                  동기 전송 여부 (true: 동기, false: 비동기)
     */
    public static void sendPizzaMessage(KafkaProducer<String, String> kafkaProducer,
            String topicName, int iterCount,
            int interIntervalMillis, int intervalMillis,
            int intervalCount, boolean sync) {

        PizzaMessage pizzaMessage = new PizzaMessage();
        int iterSeq = 0;
        long seed = 2022;
        Random random = new Random(seed);
        Faker faker = Faker.instance(random);

        long startTime = System.currentTimeMillis(); // 메시지 전송 시작 시간 기록

        // iterCount가 -1이면 무한 루프, 그렇지 않으면 iterCount만큼 반복
        while(iterSeq++ != iterCount) {
            // 가상의 피자 메시지 생성
            HashMap<String, String> pMessage = pizzaMessage.produce_msg(faker, random, iterSeq);
            // Kafka ProducerRecord 생성 (토픽, 키, 값)
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName,
                    pMessage.get("key"), pMessage.get("message"));
            // 메시지 전송
            sendMessage(kafkaProducer, producerRecord, pMessage, sync);

            // intervalCount 마다 intervalMillis 만큼 대기
            if((intervalCount > 0) && (iterSeq % intervalCount == 0)) {
                try {
                    logger.info("####### IntervalCount:{} intervalMillis:{} #########", intervalCount, intervalMillis);
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                    logger.error("Thread interrupted during interval sleep", e);
                }
            }

            // 각 메시지 전송 후 interIntervalMillis 만큼 대기
            if(interIntervalMillis > 0) {
                try {
                    logger.info("interIntervalMillis:{}", interIntervalMillis);
                    Thread.sleep(interIntervalMillis);
                } catch (InterruptedException e) {
                    logger.error("Thread interrupted during inter-interval sleep", e);
                }
            }
        }

        long endTime = System.currentTimeMillis(); // 메시지 전송 종료 시간 기록
        long timeElapsed = endTime - startTime; // 총 소요 시간 계산

        logger.info("{} milliseconds elapsed for {} iterations", timeElapsed, iterCount);
    }

    /**
     * Kafka에 메시지를 동기 또는 비동기 방식으로 전송합니다.
     *
     * @param kafkaProducer KafkaProducer 인스턴스
     * @param producerRecord 전송할 ProducerRecord
     * @param pMessage       메시지 데이터 (키, 값)
     * @param sync           동기 전송 여부 (true: 동기, false: 비동기)
     */
    public static void sendMessage(KafkaProducer<String, String> kafkaProducer,
            ProducerRecord<String, String> producerRecord,
            HashMap<String, String> pMessage, boolean sync) {
        if(!sync) {
            // 비동기 전송: 콜백을 통해 결과 처리
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("Async message - Key: {} Partition: {} Offset: {}",
                            pMessage.get("key"), metadata.partition(), metadata.offset());
                } else {
                    logger.error("Exception error from broker: {}", exception.getMessage(), exception);
                }
            });
        } else {
            try {
                // 동기 전송: 메시지 전송 완료까지 대기
                RecordMetadata metadata = kafkaProducer.send(producerRecord).get();
                logger.info("Sync message - Key: {} Partition: {} Offset: {}",
                        pMessage.get("key"), metadata.partition(), metadata.offset());
            } catch (ExecutionException e) {
                logger.error("Error occurred while sending message: {}", e.getMessage(), e);
            } catch (InterruptedException e) {
                logger.error("Message sending was interrupted: {}", e.getMessage(), e);
            }
        }
    }

    public static void main(String[] args) {

        String topicName = "pizza-topic-partitioner";

        Properties props  = new Properties();

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty("custom.specialKey", "P001"); // 특별 키 이름 설정
        props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.example.kafka.CustomPartitioner"); // 커스텀 파티셔너 클래스 설정

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        sendPizzaMessage(kafkaProducer, topicName, -1, 100, 0, 0, true);

        kafkaProducer.close();
    }
}
