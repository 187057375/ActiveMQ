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
    MQSubscribe(5, "消息订阅"),
    MQSubscribeAck(6, "消息订阅确认"),
    MQHeartBeatAck(7, "心跳确认"),
    MQBrokerAllocateRequest(8, "Broker分配请求"),
    MQBrokerAllocateResponse(9, "Broker分配请求响应"),
    MQTopicStoreRequest(10, "主题存储询问请求"),
    MQTopicStoreResponse(11, "主题存储询问请求响应");

    int messageType;

    String typeName;

    MQMsgType(int type, String typeName) {
        this.messageType = type;
        this.typeName = typeName;
    }
}
