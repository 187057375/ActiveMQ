package com.sdu.activemq.msg;

/**
 * MQ消息来源
 *
 * @author hanhan.zhang
 * */
public enum MQMsgSource {

    MQCluster(0),
    ActiveMQConsumer(1),
    MQBroker(2),
    ActiveMQProducer(3);

    int source;

    MQMsgSource(int source) {
        this.source = source;
    }

    public int getSource() {
        return this.source;
    }

}
