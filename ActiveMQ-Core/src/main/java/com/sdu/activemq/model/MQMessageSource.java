package com.sdu.activemq.model;

/**
 * MQ消息来源
 *
 * @author hanhan.zhang
 * */
public enum MQMessageSource {

    ActiveMQConsumer(1),
    ActiveMQBroker(2),
    ActiveMQProducer(3);

    int source;

    MQMessageSource(int source) {
        this.source = source;
    }

    public int getSource() {
        return this.source;
    }

}
