package com.sdu.activemq.core.transport;

import com.sdu.activemq.core.MQConfig;
import io.netty.channel.ChannelInboundHandler;
import org.apache.commons.pool.PoolableObjectFactory;

/**
 *
 * @author hanhan.zhang
 * */
public class TransportPoolFactory implements PoolableObjectFactory<DataTransport> {

    // 服务地址
    private String remoteServerAddress;

    private MQConfig mqConfig;

    private ChannelInboundHandler channelHandler;

    public TransportPoolFactory(String remoteServerAddress, MQConfig mqConfig) {
        this(remoteServerAddress, mqConfig, null);
    }

    public TransportPoolFactory(String remoteServerAddress, MQConfig mqConfig, ChannelInboundHandler channelHandler) {
        this.remoteServerAddress = remoteServerAddress;
        this.mqConfig = mqConfig;
        this.channelHandler = channelHandler;
    }

    @Override
    public DataTransport makeObject() throws Exception {
        DataTransport connector = new DataTransport(remoteServerAddress, mqConfig, channelHandler);
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
