package com.sdu.activemq.core.route;

import com.google.common.collect.Maps;
import com.sdu.activemq.core.MQConfig;
import com.sdu.activemq.network.client.NettyClient;
import com.sdu.activemq.network.client.NettyClientConfig;
import com.sdu.activemq.network.serialize.MessageObjectDecoder;
import com.sdu.activemq.network.serialize.MessageObjectEncoder;
import com.sdu.activemq.network.serialize.kryo.KryoSerializer;
import com.sdu.activemq.utils.Utils;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author hanhan.zhang
 * */
public class BrokerConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerConnector.class);

    public static final String BROKER_MESSAGE_SYNC_SOCKET_THREADS = "broker.message.sync.socket.threads";

    public static final String BROKER_MESSAGE_SYNC_SOCKET_SND_BUF = "broker.message.sync.socket.snd.buf";

    public static final String BROKER_MESSAGE_SYNC_SOCKET_RCV_BUF = "broker.message.sync.socket.rcv.buf";

    public static final String BROKER_MESSAGE_SYNC_SOCKET_EPOOL = "broker.message.sync.socket.epoll";

    // MQ Broker服务地址
    private String brokerAddress;

    private NettyClient nettyClient;

    private MQConfig mqConfig;

    private ChannelInboundHandler messageHandler = null;

    //
    private KryoSerializer serializer = new KryoSerializer();

    public BrokerConnector(String brokerAddress, MQConfig mqConfig, ChannelInboundHandler messageHandler) {
        this.brokerAddress = brokerAddress;
        this.mqConfig = mqConfig;
        this.messageHandler = messageHandler;
        doStart();
    }

    private void doStart() {
        NettyClientConfig clientConfig = new NettyClientConfig();
        clientConfig.setEPool(mqConfig.getBoolean(BROKER_MESSAGE_SYNC_SOCKET_EPOOL, false));
        clientConfig.setSocketThreads(mqConfig.getInt(BROKER_MESSAGE_SYNC_SOCKET_THREADS, 10));
        clientConfig.setClientThreadFactory(Utils.buildThreadFactory("message-sync-socket-thread-%d"));
        clientConfig.setRemoteAddress(brokerAddress);

        // Channel
        Map<ChannelOption, Object> options = Maps.newHashMap();
        options.put(ChannelOption.SO_SNDBUF, mqConfig.getInt(BROKER_MESSAGE_SYNC_SOCKET_SND_BUF, 1024));
        options.put(ChannelOption.SO_RCVBUF, mqConfig.getInt(BROKER_MESSAGE_SYNC_SOCKET_RCV_BUF, 1024));
        options.put(ChannelOption.TCP_NODELAY, true);
        options.put(ChannelOption.SO_KEEPALIVE, false);
        clientConfig.setOptions(options);

        clientConfig.setChannelHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(new MessageObjectDecoder(serializer));
                ch.pipeline().addLast(new MessageObjectEncoder(serializer));
                ch.pipeline().addLast(messageHandler);
            }
        });

        // 创建Broker Server's Client并启动
        nettyClient = new NettyClient(clientConfig);
        nettyClient.start();
    }

    public void stop() {
        if (nettyClient != null) {
            try {
                nettyClient.stop(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOGGER.error("close message connector exception", e);
            }
        }
    }
}
