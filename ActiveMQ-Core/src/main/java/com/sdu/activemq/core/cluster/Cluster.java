package com.sdu.activemq.core.cluster;

import com.sdu.activemq.core.route.BrokerConnector;
import com.sdu.activemq.model.MQMessage;

/**
 * @author hanhan.zhang
 * */
public interface Cluster {

    public BrokerConnector getConnector(MQMessage msg);

}
