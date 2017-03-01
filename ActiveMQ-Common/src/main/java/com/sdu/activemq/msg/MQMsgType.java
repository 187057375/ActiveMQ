package com.sdu.activemq.msg;

/**
 * MQ消息类型
 *
 * @author hanhan.zhang
 * */
public enum MQMsgType {

    MQHeartBeat(0, "心跳"),
    MQConsumeRequest(1, "消息消费请求"),
    MQConsumeResponse(2, "消息消费请求响应"),
    MQMsgStore(3, "消息存储"),
    MQMsgStoreAck(4, "消息存储确认"),
    MQHeartBeatAck(5, "心跳确认"),
    MQBrokerAllocateRequest(6, "Broker分配请求"),
    MQBrokerAllocateResponse(8, "Broker分配请求响应"),
    MQTopicStoreRequest(8, "主题存储询问请求"),
    MQTopicStoreResponse(9, "主题存储询问请求响应");

    int messageType;

    String typeName;

    MQMsgType(int type, String typeName) {
        this.messageType = type;
        this.typeName = typeName;
    }
}
