package kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class SimpleProducer {

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

        // Kafka에 메시지 전송
        // 별도의 Thread가 전송을 담당
        kafkaProducer.send(producerRecord);

        kafkaProducer.flush();
        kafkaProducer.close(); // 프로듀서 종료 및 리소스 정리
    };
}
