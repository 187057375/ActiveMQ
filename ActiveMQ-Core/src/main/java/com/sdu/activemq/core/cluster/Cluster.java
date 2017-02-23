package com.sdu.activemq.core.cluster;

import com.sdu.activemq.core.broker.client.BrokerTransport;
import com.sdu.activemq.model.MQMessage;

/**
 * @author hanhan.zhang
 * */
public interface Cluster {

    public void start() throws Exception;

    public BrokerTransport getConnector(MQMessage msg);

    public void destroy() throws Exception ;
}
