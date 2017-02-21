package com.sdu.activemq.core.route;

import com.sdu.activemq.core.MQConfig;
import io.netty.channel.ChannelInboundHandler;
import org.apache.commons.pool.PoolableObjectFactory;

/**
 *
 * @author hanhan.zhang
 * */
public class BrokerConnectObjectPoolFactory implements PoolableObjectFactory<BrokerConnector> {

    // MQ Broker服务地址
    private String brokerAddress;

    private MQConfig mqConfig;

    private ChannelInboundHandler channelHandler;

    public BrokerConnectObjectPoolFactory(String brokerAddress, MQConfig mqConfig, ChannelInboundHandler channelHandler) {
        this.brokerAddress = brokerAddress;
        this.mqConfig = mqConfig;
        this.channelHandler = channelHandler;
    }

    @Override
    public BrokerConnector makeObject() throws Exception {
        BrokerConnector connector = new BrokerConnector(brokerAddress, mqConfig, channelHandler);
        return connector;
    }

    @Override
    public void destroyObject(BrokerConnector connector) throws Exception {
        connector.stop();
    }

    @Override
    public boolean validateObject(BrokerConnector connector) {
        return true;
    }

    @Override
    public void activateObject(BrokerConnector connector) throws Exception {

    }

    @Override
    public void passivateObject(BrokerConnector connector) throws Exception {

    }
}
