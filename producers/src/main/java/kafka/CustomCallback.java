package kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomCallback implements Callback {

    public static final Logger logger = LoggerFactory.getLogger(CustomCallback.class.getName());

    private int seq;

    public CustomCallback(int seq) {
        this.seq = seq;
    }

    /**
     * 메시지 전송 완료 시 호출되는 메서드
     *
     * @param recordMetadata 전송된 메시지의 메타데이터
     * @param exception      전송 중 발생한 예외 (없으면 null)
     */
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
        if (exception == null) {
            logger.info("seq: {} partition: {} offset: {}", seq, recordMetadata.partition(), recordMetadata.offset());
        } else {
            logger.error("exception error from broker" + exception.getMessage());
        }
    }
}
