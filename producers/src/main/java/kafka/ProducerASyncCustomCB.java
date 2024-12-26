package kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerASyncCustomCB {

    public static final Logger logger = LoggerFactory.getLogger(ProducerASyncCustomCB.class.getName());

    public static void main(String[] args) {

        // 전송할 Kafka 토픽 이름
        String topicName = "multipart-topic";

        // 프로듀서 설정을 담을 Properties 객체 생성
        Properties props = new Properties();

        // Kafka 브로커 주소 설정
        props.setProperty("bootstrap.servers", "192.168.56.101:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");

        // 메시지 키를 Integer로, 값을 String으로 직렬화 설정
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer 인스턴스 생성(키 타입을 Integer로 변경)
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<Integer, String>(props);

        for (int i = 0; i < 20; i++) {
            // 전송할 메시지 생성(Key, Value)
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicName, i, "Hello World");

            // CustomCallback 인스턴스 생성
            Callback callback = new CustomCallback(i);

            // Kafka에 메시지 비동기 전송 및 콜백 등록
            kafkaProducer.send(producerRecord, callback);
        }

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
