package kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleProducerASync {
    public static final Logger logger = LoggerFactory.getLogger(SimpleProducerASync.class.getName());
    public static void main(String[] args) {

        // 전송할 Kafka 토픽 이름
        String topicName = "simple-topic";

        // 프로듀서 설정을 담을 Properties 객체 생성
        Properties props = new Properties();

        // Kafka 브로커 주소 설정
        props.setProperty("bootstrap.servers", "192.168.56.101:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");

        // 메시지 키와 값을 문자열로 직렬화하기 위한 설정
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer 인스턴스 생성
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        // 전송할 메시지 생성
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "id-001", "Hello World");

        // Kafka에 메시지 전송 V1: 익명 내부 클래스 사용
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

        // Kafka에 메시지 전송 V2: 람다 표현식 사용
        kafkaProducer.send(producerRecord, (recordMetadata, exception) -> {
            if (exception == null) {
                logger.info("\n ##### record metadata received ##### \n" +
                        "partition: " + recordMetadata.partition() + "\n" +
                        "offset: " + recordMetadata.offset() + "\n" +
                        "timestamp: " + recordMetadata.timestamp());
            } else {
                logger.error("exception error from broker" + exception.getMessage());
            }
        });

        // 비동기 전송 후 약간의 지연을 줘서 콜백이 처리될 시간을 확보
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // 프로듀서 종료 및 리소스 정리
        kafkaProducer.close();
    };
}
