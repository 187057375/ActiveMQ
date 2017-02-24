package com.sdu.activemq.model;

/**
 * MQ消息类型
 *
 * @author hanhan.zhang
 * */
public enum MQMsgType {

    MQHeartBeat(0, "心跳"),
    MQMessageRequest(1, "消息请求"),
    MQMessageResponse(2, "消息响应"),
    MQMessageStore(3, "消息存储"),
    MQMessageStoreAck(4, "消息存储确认"),
    MQSubscribe(5, "消息订阅"),
    MQSubscribeAck(6, "消息订阅确认"),
    MQHeartBeatAck(7, "心跳确认");

    int messageType;

    String typeName;

    MQMsgType(int type, String typeName) {
        this.messageType = type;
        this.typeName = typeName;
    }
}
