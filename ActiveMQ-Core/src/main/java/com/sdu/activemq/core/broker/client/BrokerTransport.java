package com.sdu.activemq.core.broker.client;

import com.google.common.collect.Maps;
import com.sdu.activemq.core.MQConfig;
import com.sdu.activemq.msg.MQMessage;
import com.sdu.activemq.msg.MsgHeartBeat;
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

import static com.sdu.activemq.msg.MQMsgSource.MQCluster;
import static com.sdu.activemq.msg.MQMsgType.MQHeartBeat;


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


    // MQ Broker服务地址
    private String brokerAddress;

    private NettyClient nettyClient;

    private MQConfig mqConfig;

    private ChannelInboundHandler messageHandler = null;

    public BrokerTransport(String brokerAddress, MQConfig mqConfig, ChannelInboundHandler messageHandler) {
        this.brokerAddress = brokerAddress;
        this.mqConfig = mqConfig;
        this.messageHandler = messageHandler;
        doStart();
    }

    /**
     * Broker Server创建客户端连接
     * */
    private void doStart() {
        NettyClientConfig clientConfig = new NettyClientConfig();
        clientConfig.setEPool(false);
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
                KryoSerializer serializer = new KryoSerializer(MQMessage.class);
                ch.pipeline().addLast(new IdleStateHandler(1, 4, 0, TimeUnit.MINUTES));
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

    public ChannelFuture writeAndFlush(Object msg) {
        return nettyClient.writeAndFlush(msg);
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

        MsgHeartBeat msg = new MsgHeartBeat("heat bear message");

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            super.userEventTriggered(ctx, evt);
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent stateEvent = (IdleStateEvent) evt;
                if (stateEvent.state() == IdleState.READER_IDLE) {
                    // 心跳消息
                    MQMessage mqMessage = new MQMessage(MQHeartBeat, MQCluster, msg);
                    ctx.writeAndFlush(mqMessage);
                }
            }
        }
    }
}
