package com.example.kafka;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CustomPartitioner는 메시지 키에 따라 파티션을 커스터마이징하여
 * 특정 키에 대해 지정된 파티션 그룹으로 메시지를 분배합니다.
 */
public class CustomPartitioner implements Partitioner {

    public static final Logger logger = LoggerFactory.getLogger(CustomPartitioner.class.getName());

    private String specialKeyName;

    /**
     * 파티셔너 설정을 구성합니다.
     *
     * @param configs 파티셔너 설정을 담은 맵
     */
    @Override
    public void configure(Map<String, ?> configs) {
        specialKeyName = configs.get("custom.specialKey").toString();
    }

    /**
     * 메시지의 키에 따라 파티션을 결정합니다.
     *
     * @param topic         메시지가 전송될 토픽 이름
     * @param key           메시지의 키 객체
     * @param keyBytes      직렬화된 메시지 키 바이트 배열
     * @param value         메시지의 값 객체
     * @param valueBytes    직렬화된 메시지 값 바이트 배열
     * @param cluster       클러스터 메타데이터 정보
     * @return              메시지를 전송할 파티션 번호
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 토픽의 모든 파티션 정보를 가져옴
        List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);

        int numPartitions = partitionInfoList.size();
        int numSpecialPartitions = (int)(numPartitions * 0.5); // 특별 키가 전송될 파티션 수
        int partitionIndex = 0;

        // 키가 null인 경우 예외 발생
        if (keyBytes == null) {
            throw new InvalidRecordException("key should not be null");
        }

        if(((String)key).equals(specialKeyName)) {
            // 특별 키인 경우, 첫 절반 파티션 중 하나로 매핑
            partitionIndex = Utils.toPositive(Utils.murmur2(valueBytes)) % numSpecialPartitions;
        } else {
            // 일반 키인 경우, 나머지 절반 파티션 중 하나로 매핑
            partitionIndex = Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - numSpecialPartitions) + numSpecialPartitions;
        }

        logger.info("key: {} is sent to partition: {}", key.toString(), partitionIndex);

        return partitionIndex;
    }

    @Override
    public void close() {

    }
}
