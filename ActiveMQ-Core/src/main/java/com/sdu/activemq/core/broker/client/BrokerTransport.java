package com.sdu.activemq.core.broker.client;

import com.google.common.collect.Maps;
import com.sdu.activemq.core.MQConfig;
import com.sdu.activemq.model.MQMessage;
import com.sdu.activemq.model.msg.HeartBeatMsg;
import com.sdu.activemq.network.client.NettyClient;
import com.sdu.activemq.network.client.NettyClientConfig;
import com.sdu.activemq.network.serialize.MessageObjectDecoder;
import com.sdu.activemq.network.serialize.MessageObjectEncoder;
import com.sdu.activemq.network.serialize.kryo.KryoSerializer;
import com.sdu.activemq.utils.Utils;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.sdu.activemq.model.MQMsgSource.ActiveMQCluster;
import static com.sdu.activemq.model.MQMsgType.ActiveMQHeatBeat;

/**
 * Broker Server服务客户端
 *
 * @author hanhan.zhang
 * */
public class BrokerTransport {

    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerTransport.class);

    // Broker Server客户端Socket IO线程数
    private static final String BROKER_TRANSPORT_SOCKET_THREADS = "broker.transport.socket.threads";

    // Broker Server客户端Socket发送缓冲区
    private static final String BROKER_TRANSPORT_SOCKET_SND_BUF = "broker.transport.socket.snd.buf";

    // Broker Server客户端Socket接收缓冲区
    private static final String BROKER_TRANSPORT_SOCKET_RCV_BUF = "broker.transport.socket.rcv.buf";

    private static final String BROKER_TRANSPORT_SOCKET_EPOOL = "broker.transport.socket.epoll";

    // MQ Broker服务地址
    private String brokerAddress;

    private NettyClient nettyClient;

    private MQConfig mqConfig;

    private ChannelInboundHandler messageHandler = null;

    //
    private KryoSerializer serializer = new KryoSerializer();

    public BrokerTransport(String brokerAddress, MQConfig mqConfig, ChannelInboundHandler messageHandler) {
        this.brokerAddress = brokerAddress;
        this.mqConfig = mqConfig;
        this.messageHandler = messageHandler;
        doStart();
    }

    private void doStart() {
        NettyClientConfig clientConfig = new NettyClientConfig();
        clientConfig.setEPool(mqConfig.getBoolean(BROKER_TRANSPORT_SOCKET_EPOOL, false));
        clientConfig.setSocketThreads(mqConfig.getInt(BROKER_TRANSPORT_SOCKET_THREADS, 10));
        clientConfig.setClientThreadFactory(Utils.buildThreadFactory("message-sync-socket-thread-%d"));
        clientConfig.setRemoteAddress(brokerAddress);

        // Channel
        Map<ChannelOption, Object> options = Maps.newHashMap();
        options.put(ChannelOption.SO_SNDBUF, mqConfig.getInt(BROKER_TRANSPORT_SOCKET_SND_BUF, 1024));
        options.put(ChannelOption.SO_RCVBUF, mqConfig.getInt(BROKER_TRANSPORT_SOCKET_RCV_BUF, 1024));
        options.put(ChannelOption.TCP_NODELAY, true);
        options.put(ChannelOption.SO_KEEPALIVE, false);
        clientConfig.setOptions(options);

        clientConfig.setChannelHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                // 心跳[1秒内若是无数据读取, 则发送心跳]
                ch.pipeline().addLast(new IdleStateHandler(1, 4, 0, TimeUnit.SECONDS));
                ch.pipeline().addLast(new MessageObjectDecoder(serializer));
                ch.pipeline().addLast(new MessageObjectEncoder(serializer));
                ch.pipeline().addLast(new HeartBeatHandler());
                ch.pipeline().addLast(messageHandler);
            }
        });

        // 创建Broker Server's Client并启动
        nettyClient = new NettyClient(clientConfig);
        nettyClient.start();

        if (nettyClient.isStarted()) {
            LOGGER.info("transport connect broker server[{}] success .", brokerAddress);
        }
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

    private class HeartBeatHandler extends ChannelInboundHandlerAdapter {

        HeartBeatMsg msg = new HeartBeatMsg(Utils.socketAddressCastString(nettyClient.getLocalSocketAddress()));

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            super.userEventTriggered(ctx, evt);
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent stateEvent = (IdleStateEvent) evt;
                if (stateEvent.state() == IdleState.READER_IDLE) {
                    // 心跳消息
                    MQMessage mqMessage = new MQMessage(ActiveMQHeatBeat, ActiveMQCluster, msg);
                    ctx.writeAndFlush(mqMessage);
                }
            }
        }
    }
}
