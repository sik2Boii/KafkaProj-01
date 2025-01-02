package com.practice.kafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.practice.kafka.model.OrderModel;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OrderModel 객체를 JSON 바이트 배열로 직렬화하는 Kafka Serializer 구현 클래스
 */
public class OrderSerializer implements Serializer<OrderModel> {

    public static final Logger logger = LoggerFactory.getLogger(OrderSerializer.class.getName());

    // ObjectMapper는 날짜/시간 타입을 기본으로 처리하지 못하기 때문에,
    // LocalDateTime 등의 필드를 정확하게 JSON으로 직렬화하기 위해 JavaTimeModule을 등록
    ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Override
    public byte[] serialize(String topic, OrderModel orderModel) {
        byte[] serializerOrder = null;

        try {
            // OrderModel 객체를 JSON 바이트 배열로 변환
            serializerOrder = objectMapper.writeValueAsBytes(orderModel);
        } catch (JsonProcessingException e) {
            logger.error("Json processing exception", e.getMessage());
        }
        return serializerOrder;
    }
}
