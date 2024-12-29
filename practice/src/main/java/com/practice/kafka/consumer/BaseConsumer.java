package com.practice.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * BaseConsumer는 일반적인 Kafka 소비자 기능을 캡슐화한 클래스입니다.
 * 제네릭 타입 <K, V>를 사용하여 키와 값의 타입을 지정할 수 있습니다.
 *
 * @param <K> 메시지 키의 타입
 * @param <V> 메시지 값의 타입
 */
public class BaseConsumer<K extends Serializable, V extends Serializable> {

    public static final Logger logger = LoggerFactory.getLogger(BaseConsumer.class.getName());

    private KafkaConsumer<K, V> kafkaConsumer;
    private List<String> topics;

    /**
     * BaseConsumer 생성자
     *
     * @param consumerProps 소비자 설정을 담은 Properties 객체
     * @param topics        구독할 Kafka 토픽 목록
     */
    public BaseConsumer(Properties consumerProps, List<String> topics) {
        this.kafkaConsumer = new KafkaConsumer<K, V>(consumerProps);
        this.topics = topics;
    }

    /**
     * 소비자를 초기화하고 토픽을 구독하며, 종료 시점을 위한 Shutdown Hook을 등록합니다.
     */
    public void initConsumer() {
        this.kafkaConsumer.subscribe(this.topics);
        shutdownHookToRuntime(this.kafkaConsumer);
    }

    /**
     * 애플리케이션 종료 시 KafkaConsumer를 안전하게 종료하기 위한 Shutdown Hook을 등록합니다.
     *
     * @param kafkaConsumer 종료할 KafkaConsumer 인스턴스
     */
    private void shutdownHookToRuntime(KafkaConsumer<K, V> kafkaConsumer) {
        // main thread
        Thread mainThread = Thread.currentThread();

        // main thread 종료시 별도의 thread로 KafkaConsumer wakeup()메소드를 호출하게 함.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info(" main program starts to exit by calling wakeup");
                kafkaConsumer.wakeup();

                try {
                    mainThread.join();
                } catch(InterruptedException e) { e.printStackTrace();}
            }
        });
    }

    /**
     * 단일 ConsumerRecord를 처리하는 메서드
     *
     * @param record 처리할 ConsumerRecord 객체
     */
    private void processRecord(ConsumerRecord<K, V> record) {
        logger.info("record key:{},  partition:{}, record offset:{} record value:{}",
                record.key(), record.partition(), record.offset(), record.value());
    }

    /**
     * 여러 ConsumerRecords를 처리하는 메서드
     *
     * @param records 처리할 ConsumerRecords 객체
     */
    private void processRecords(ConsumerRecords<K, V> records) {
        // 반복문
//        for (ConsumerRecord<K, V> record : records) {
//            processRecord(record);
//        }

        // 람다식
        records.forEach(record -> processRecord(record));
    }

    /**
     * 지정된 커밋 모드에 따라 메시지를 폴링하고 커밋하는 메서드
     *
     * @param durationMillis 폴링 간격 (밀리초 단위)
     * @param commitMode     커밋 모드 ("sync" 또는 "async")
     */
    public void pollConsumes(long durationMillis, String commitMode) {
        try {
            while (true) {
                if (commitMode.equals("sync")) {
                    pollCommitSync(durationMillis);
                } else {
                    pollCommitAsync(durationMillis);
                }
            }
        }catch(WakeupException e) {
            logger.error("wakeup exception has been called");
        }catch(Exception e) {
            logger.error(e.getMessage());
        }finally {
            logger.info("##### commit sync before closing");
            kafkaConsumer.commitSync();
            logger.info("finally consumer is closing");
            closeConsumer();
        }
    }

    /**
     * 비동기 커밋 방식을 사용하여 메시지를 폴링하고 커밋하는 메서드
     *
     * @param durationMillis 폴링 간격 (밀리초 단위)
     * @throws WakeupException wakeup() 호출 시 발생
     * @throws Exception        기타 예외
     */
    private void pollCommitAsync(long durationMillis) throws WakeupException, Exception {
        ConsumerRecords<K, V> consumerRecords = this.kafkaConsumer.poll(Duration.ofMillis(durationMillis));
        processRecords(consumerRecords);
        // 비동기 커밋
        this.kafkaConsumer.commitAsync( (offsets, exception) -> {
            if(exception != null) {
                logger.error("offsets {} is not completed, error:{}", offsets, exception.getMessage());
            }

        });
    }

    /**
     * 동기 커밋 방식을 사용하여 메시지를 폴링하고 커밋하는 메서드
     *
     * @param durationMillis 폴링 간격 (밀리초 단위)
     * @throws WakeupException wakeup() 호출 시 발생
     * @throws Exception        기타 예외
     */
    private void pollCommitSync(long durationMillis) throws WakeupException, Exception {
        ConsumerRecords<K, V> consumerRecords = this.kafkaConsumer.poll(Duration.ofMillis(durationMillis));
        processRecords(consumerRecords);
        try {
            if(consumerRecords.count() > 0 ) {
                // 동기 커밋
                this.kafkaConsumer.commitSync();
                logger.info("commit sync has been called");
            }
        } catch(CommitFailedException e) {
            logger.error(e.getMessage());
        }
    }

    /**
     * KafkaConsumer를 종료하는 메서드
     */
    public void closeConsumer() {
        this.kafkaConsumer.close();
    }

    /**
     * 메인 메서드 - BaseConsumer를 테스트하기 위한 간단한 실행 예제
     *
     * @param args 명령줄 인수
     */
    public static void main(String[] args) {

        String topicName = "file-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "file-group");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        BaseConsumer<String, String> baseConsumer = new BaseConsumer<String, String>(props, List.of(topicName));
        baseConsumer.initConsumer();
        String commitMode = "async";

        baseConsumer.pollConsumes(100, commitMode);
        baseConsumer.closeConsumer();

    }
}