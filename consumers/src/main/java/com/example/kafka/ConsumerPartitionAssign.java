package com.example.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerPartitionAssign {

    public static final Logger logger = LoggerFactory.getLogger(ConsumerPartitionAssign.class.getName());

    public static void main(String[] args) {

        String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_pizza_assign_seek");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
        TopicPartition topicPartition = new TopicPartition(topicName, 0);
        kafkaConsumer.assign(Arrays.asList(topicPartition)); // 특정 토픽과 파티션을 지정하여 소비자에게 할당

        // main thread
        Thread mainThread = Thread.currentThread();

        // main thread 종료시 별도의 thread로 KafkaConsumer wakeup()메소드를 호출하게 함
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("main program starts to exit by calling wakeup");
                kafkaConsumer.wakeup();

                try {
                    mainThread.join();
                } catch(InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        pollCommitSync(kafkaConsumer);
    }

    private static void pollCommitAsync(KafkaConsumer<String, String> kafkaConsumer) {

        int loopCnt = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info(" ##### loopCnt: {}, consumerRecords: {} #####", loopCnt++, consumerRecords.count());

                for (ConsumerRecord record : consumerRecords) {
                    logger.info("record key: {},  partition: {}, record offset: {} record value: {}", record.key(), record.partition(), record.offset(), record.value());
                }

                // 오프셋을 비동기적으로 커밋하고, 커밋 결과를 콜백으로 처리
                // 비동기 커밋을 사용하면 성능이 향상되지만, 커밋 실패 시 이를 처리할 필요가 있음
                kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if (exception != null) {
                            logger.error("offsets {} is not completed, error: {}", offsets, exception);
                        }
                    }
                });
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            try {
                // 프로그램 종료 시, 아직 커밋되지 않은 오프셋을 동기적으로 커밋하여 데이터 일관성 보장
                kafkaConsumer.commitSync();
            } catch (CommitFailedException e) {
                logger.error("Final commit failed: {}", e.getMessage());
            } finally {
                logger.info("finally consumer is closing");
                kafkaConsumer.close();
            }
        }
    }

    private static void pollCommitSync(KafkaConsumer<String, String> kafkaConsumer) {

        int loopCnt = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info(" ##### loopCnt: {}, consumerRecords: {} #####", loopCnt++, consumerRecords.count());

                for (ConsumerRecord record : consumerRecords) {
                    logger.info("record key: {},  partition: {}, record offset: {} record value: {}", record.key(), record.partition(), record.offset(), record.value());
                }

                try {
                    if (consumerRecords.count() > 0) {
                        // 메시지를 하나씩 커밋하면 성능이 저하되므로, 배치 단위로 동기 커밋
                        kafkaConsumer.commitSync();
                        logger.info("commit sync has been called");
                    }
                } catch (CommitFailedException e) {
                    logger.error(e.getMessage());
                }
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }
    }

    public static void pollAutoCommit(KafkaConsumer<String, String> kafkaConsumer) {

        int loopCnt = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info(" ##### loopCnt: {}, consumerRecords: {} #####", loopCnt++, consumerRecords.count());

                for (ConsumerRecord record : consumerRecords) {
                    logger.info("record key: {},  partition: {}, record offset: {} record value: {}", record.key(), record.partition(), record.offset(), record.value());
                }

                try {
                    logger.info("main thread is sleeping {} ms during while loop", 10000);
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } finally {
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }
    }
}