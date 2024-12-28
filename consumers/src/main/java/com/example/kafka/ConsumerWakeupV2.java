package com.example.kafka;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerWakeupV2 {

    public static final Logger logger = LoggerFactory.getLogger(ConsumerWakeupV2.class.getName());

    public static void main(String[] args) {

        String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-02");
        props.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000"); // poll() 호출 사이의 최대 대기 시간 (60초)

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
        kafkaConsumer.subscribe(List.of(topicName));

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

        int loopCnt = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info(" ##### loopCnt: {}, consumerRecords: {} #####", loopCnt++, consumerRecords.count());

                for (ConsumerRecord record : consumerRecords) {
                    logger.info("record key: {},  partition: {}, record offset: {} record value: {}", record.key(), record.partition(), record.offset(), record.value());
                }

                try {
                    // 각 루프 후, 루프 카운터에 비례하여 대기 시간 설정
                    // 최대 대기 시간 60초이므로 6번째 반복 이후에는 대기 시간이 60초를 초과하여 consumer가 종료됩니다.
                    logger.info("main thread is sleeping {} ms during while loop", loopCnt * 10000);
                    Thread.sleep(loopCnt * 10000);
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