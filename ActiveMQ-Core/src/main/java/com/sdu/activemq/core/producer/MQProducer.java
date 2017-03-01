package com.sdu.activemq.core.producer;

import com.lmax.disruptor.dsl.ProducerType;
import com.sdu.activemq.core.MQConfig;
import com.sdu.activemq.core.cluster.BrokerNodeCluster;
import com.sdu.activemq.core.disruptor.DisruptorQueue;
import com.sdu.activemq.msg.MQMessage;

/**
 * @author hanhan.zhang
 * */
public class MQProducer {

    private BrokerNodeCluster cluster;

    // 消息队列
    private DisruptorQueue queue;

    public MQProducer(MQConfig mqConfig) {
        cluster = BrokerNodeCluster.getCluster(mqConfig);
        queue = new DisruptorQueue(ProducerType.MULTI, new MessageSender(cluster), mqConfig);
    }

    public void sendMsg(MQMessage mqMessage) {
        queue.publish(mqMessage);
    }

    public void close() throws Exception {
        cluster.destroy();
    }
}
