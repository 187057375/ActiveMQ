package com.sdu.activemq.core.cluster;

import com.sdu.activemq.core.route.BrokerConnector;
import com.sdu.activemq.model.MQMessage;

import java.util.concurrent.ConcurrentHashMap;

/**
 * BrokerCluster职责:
 *
 *  1: 路由MQ消息
 *
 *  2: 探测Broker存活
 *
 *
 * @author hanhan.zhang
 * */
public class BrokerCluster implements Cluster {

    // Broker链接表
    private ConcurrentHashMap<BrokerNode, BrokerConnector> connectors;

    @Override
    public BrokerConnector getConnector(MQMessage msg) {
        return null;
    }
}
