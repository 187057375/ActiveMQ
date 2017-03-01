package com.sdu.activemq.core.consumer;

import com.sdu.activemq.core.MQConfig;
import com.sdu.activemq.core.cluster.BrokerNodeCluster;
import com.sdu.activemq.core.cluster.DataChangeListener;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 *
 * @author hanhan.zhang
 * */
public class MQConsumer {

    private String topic;

    private String topicGroup;

    private MQConfig mqConfig;

    private BrokerNodeCluster cluster;

    public MQConsumer(String topic, String topicGroup, MQConfig mqConfig) {
        this.topic = topic;
        this.topicGroup = topicGroup;
        this.mqConfig = mqConfig;
        cluster = BrokerNodeCluster.getCluster(mqConfig);
        cluster.addDataChangeListener(new DataChangeListenerImpl());
    }

    private class MessageConsumeHandler extends ChannelInboundHandlerAdapter {

    }

    private class DataChangeListenerImpl implements DataChangeListener {

    }
}
