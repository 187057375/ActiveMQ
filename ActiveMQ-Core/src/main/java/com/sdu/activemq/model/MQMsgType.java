package com.sdu.activemq.model;

/**
 * MQ消息类型
 *
 * @author hanhan.zhang
 * */
public enum MQMsgType {

    ActiveMQHeatBeat(0),
    ActiveMQSubscribe(1),
    ActiveMQUnsubscribe(2),
    ActiveMQMessage(3),
    ActiveMQProducerAck(4),
    ActiveMQConsumerAck(5);

    int messageType;

    MQMsgType(int type) {
        this.messageType = type;
    }

    public int getSource() {
        return this.messageType;
    }

}
