package com.sdu.activemq.core.transport;

import com.sdu.activemq.core.MQConfig;
import io.netty.channel.ChannelInboundHandler;
import org.apache.commons.pool.PoolableObjectFactory;

/**
 *
 * @author hanhan.zhang
 * */
public class TransportPoolFactory implements PoolableObjectFactory<DataTransport> {

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
    public DataTransport makeObject() throws Exception {
        DataTransport connector = new DataTransport(brokerAddress, mqConfig, channelHandler);
        return connector;
    }

    @Override
    public void destroyObject(DataTransport connector) throws Exception {
        connector.stop();
    }

    @Override
    public boolean validateObject(DataTransport connector) {
        return true;
    }

    @Override
    public void activateObject(DataTransport connector) throws Exception {

    }

    @Override
    public void passivateObject(DataTransport connector) throws Exception {

    }
}
