package com.practice.kafka.model;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * 주문 정보를 나타내는 도메인 모델 클래스입니다.
 * 이 클래스는 비즈니스 로직과 데이터베이스와의 상호작용을 담당하며,
 * 모든 주문 관련 필드를 포함합니다.
 * 반면, DTO(Data Transfer Object)는 필요한 데이터만을 포함하여
 * 계층 간 데이터 전송을 단순화합니다.
 */
public class OrderModel implements Serializable {

    public String orderId;
    public String shopId;
    public String menuName;
    public String userName;
    public String phoneNumber;
    public String address;
    public LocalDateTime orderTime;

    public OrderModel(String orderId, String shopId, String menuName, String userName,
                    String phoneNumber, String address, LocalDateTime orderTime) {

        this.orderId = orderId;
        this.shopId = shopId;
        this.menuName = menuName;
        this.userName = userName;
        this.phoneNumber = phoneNumber;
        this.address = address;
        this.orderTime = orderTime;
    }

    public OrderModel() {

    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getShopId() {
        return shopId;
    }

    public void setShopId(String shopId) {
        this.shopId = shopId;
    }

    public String getMenuName() {
        return menuName;
    }

    public void setMenuName(String menuName) {
        this.menuName = menuName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public LocalDateTime getOrderTime() {
        return orderTime;
    }

    public void setOrderTime(LocalDateTime orderTime) {
        this.orderTime = orderTime;
    }

    @Override
    public String toString() {
        return "OrderModel{" +
                "orderId='" + orderId + '\'' +
                ", shopId='" + shopId + '\'' +
                ", menuName='" + menuName + '\'' +
                ", userName='" + userName + '\'' +
                ", phoneNumber='" + phoneNumber + '\'' +
                ", address='" + address + '\'' +
                ", orderTime=" + orderTime +
                '}';
    }
}