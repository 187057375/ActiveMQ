package com.sdu.activemq.model;

/**
 * MQ消息类型
 *
 * @author hanhan.zhang
 * */
public enum MQMsgType {

    MQHeartBeat(0, "心跳"),
    MQSMessageRequest(1, "消息请求"),
    MQUnsubscribe(2, "取消订阅"),
    MQMessageStore(3, "消息存储"),
    MQMessageAck(4, "消息确认"),
    MQConsumeAck(5, "消息消费确认"),
    MQHeartBeatAck(6, "心跳确认");

    int messageType;

    String typeName;

    MQMsgType(int type, String typeName) {
        this.messageType = type;
        this.typeName = typeName;
    }
}
