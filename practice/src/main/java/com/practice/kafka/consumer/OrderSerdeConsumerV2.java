package com.practice.kafka.consumer;

import com.practice.kafka.model.OrderModel;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OrderSerdeConsumerV2 클래스는 특정 타입으로 고정되어 있어 코드가 단순해지고, 특정 용도에 최적화되어 있습니다.
 * 단일 타입만을 처리할 필요가 있을 때 권장됩니다.
 */
public class OrderSerdeConsumerV2 {

    public static final Logger logger = LoggerFactory.getLogger(OrderSerdeConsumerV2.class.getName());

    private KafkaConsumer<String, OrderModel> kafkaConsumer;
    private List<java.lang.String> topics;

    public OrderSerdeConsumerV2(Properties consumerProps, List<String> topics) {
        this.kafkaConsumer = new KafkaConsumer<String, OrderModel>(consumerProps);
        this.topics = topics;
    }

    public void initConsumer() {
        this.kafkaConsumer.subscribe(this.topics);
        shutdownHookToRuntime(this.kafkaConsumer);
    }

    private void shutdownHookToRuntime(KafkaConsumer<String, OrderModel> kafkaConsumer) {
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

    private void processRecord(ConsumerRecord<String, OrderModel> record) {
        logger.info("record key:{},  partition:{}, record offset:{} record value:{}",
                record.key(), record.partition(), record.offset(), record.value());
    }

    private void processRecords(ConsumerRecords<String, OrderModel> records) {
        records.forEach(record -> processRecord(record));
    }

    public void pollConsumes(long durationMillis, java.lang.String commitMode) {
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

    private void pollCommitAsync(long durationMillis) throws WakeupException, Exception {
        ConsumerRecords<String, OrderModel> consumerRecords = this.kafkaConsumer.poll(Duration.ofMillis(durationMillis));
        processRecords(consumerRecords);
        // 비동기 커밋
        this.kafkaConsumer.commitAsync( (offsets, exception) -> {
            if(exception != null) {
                logger.error("offsets {} is not completed, error:{}", offsets, exception.getMessage());
            }

        });
    }

    private void pollCommitSync(long durationMillis) throws WakeupException, Exception {
        ConsumerRecords<String, OrderModel> consumerRecords = this.kafkaConsumer.poll(Duration.ofMillis(durationMillis));
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

    public void closeConsumer() {
        this.kafkaConsumer.close();
    }

    public static void main(java.lang.String[] args) {

        java.lang.String topicName = "order-serde-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OrderDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "order-serde-topic");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        OrderSerdeConsumerV2 orderSerdeConsumerV2 = new OrderSerdeConsumerV2(props, List.of(topicName));
        orderSerdeConsumerV2.initConsumer();
        java.lang.String commitMode = "async";

        orderSerdeConsumerV2.pollConsumes(100, commitMode);
        orderSerdeConsumerV2.closeConsumer();

    }
}