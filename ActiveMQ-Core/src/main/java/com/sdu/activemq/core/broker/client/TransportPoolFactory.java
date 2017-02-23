package com.sdu.activemq.core.broker.client;

import com.sdu.activemq.core.MQConfig;
import io.netty.channel.ChannelInboundHandler;
import org.apache.commons.pool.PoolableObjectFactory;

/**
 *
 * @author hanhan.zhang
 * */
public class TransportPoolFactory implements PoolableObjectFactory<BrokerTransport> {

    // MQ Broker服务地址
    private String brokerAddress;

    private MQConfig mqConfig;

    private ChannelInboundHandler channelHandler;

    public TransportPoolFactory(String brokerAddress, MQConfig mqConfig, ChannelInboundHandler channelHandler) {
        this.brokerAddress = brokerAddress;
        this.mqConfig = mqConfig;
        this.channelHandler = channelHandler;
    }

    @Override
    public BrokerTransport makeObject() throws Exception {
        BrokerTransport connector = new BrokerTransport(brokerAddress, mqConfig, channelHandler);
        return connector;
    }

    @Override
    public void destroyObject(BrokerTransport connector) throws Exception {
        connector.stop();
    }

    @Override
    public boolean validateObject(BrokerTransport connector) {
        return true;
    }

    @Override
    public void activateObject(BrokerTransport connector) throws Exception {

    }

    @Override
    public void passivateObject(BrokerTransport connector) throws Exception {

    }
}
