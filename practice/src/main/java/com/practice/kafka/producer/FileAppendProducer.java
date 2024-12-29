package com.practice.kafka.producer;

import com.practice.kafka.event.EventHandler;
import com.practice.kafka.event.FileEventHandler;
import com.practice.kafka.event.FileEventSource;
import java.io.File;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileAppendProducer는 파일의 변경을 모니터링하고,
 * 새로운 라인이 추가될 때마다 이를 Kafka 토픽으로 전송하는 역할을 합니다.
 */
public class FileAppendProducer {

    public static final Logger logger = LoggerFactory.getLogger(FileAppendProducer.class.getName());

    public static void main(String[] args) {

        String topicName = "file-topic";
        File file = new File("C:\\Users\\Junsik\\workspace\\KafkaProj-01\\practice\\src\\main\\resources\\pizza_append.txt");
        boolean sync = false;

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.56.101:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        EventHandler eventHandler = new FileEventHandler(kafkaProducer, topicName, sync);
        FileEventSource fileEventSource = new FileEventSource(1000, file, eventHandler);
        Thread fileEventSourceThread = new Thread(fileEventSource);
        fileEventSourceThread.start();

        try {
            fileEventSourceThread.join();
        } catch (InterruptedException e) {
            logger.info(e.getMessage());
        } finally {
            kafkaProducer.close();
        }
    }

}
