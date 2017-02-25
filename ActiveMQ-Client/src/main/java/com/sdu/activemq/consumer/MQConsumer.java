package com.sdu.activemq.consumer;

import com.google.common.collect.Maps;
import com.sdu.activemq.msg.*;
import com.sdu.activemq.network.client.NettyClient;
import com.sdu.activemq.network.client.NettyClientConfig;
import com.sdu.activemq.network.serialize.MessageObjectDecoder;
import com.sdu.activemq.network.serialize.MessageObjectEncoder;
import com.sdu.activemq.network.serialize.kryo.KryoSerializer;
import com.sdu.activemq.producer.MQProducer;
import com.sdu.activemq.util.Utils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static com.sdu.activemq.msg.MQMsgSource.ActiveMQConsumer;
import static com.sdu.activemq.msg.MQMsgType.MQSubscribe;

/**
 * @author hanhan.zhang
 * */
public class MQConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MQProducer.class);

    private String clusterAddress;

    private NettyClient nettyClient;

    public MQConsumer(String clusterAddress) {
        this.clusterAddress = clusterAddress;
    }

    public void start() {
        NettyClientConfig clientConfig = new NettyClientConfig();
        clientConfig.setEPool(false);
        clientConfig.setSocketThreads(10);
        clientConfig.setClientThreadFactory(Utils.buildThreadFactory("producer-socket-thread-%d"));
        clientConfig.setRemoteAddress(clusterAddress);

        // Channel
        Map<ChannelOption, Object> options = Maps.newHashMap();
        options.put(ChannelOption.SO_SNDBUF, 1024);
        options.put(ChannelOption.SO_RCVBUF, 1024);
        options.put(ChannelOption.TCP_NODELAY, true);
        options.put(ChannelOption.SO_KEEPALIVE, false);
        clientConfig.setOptions(options);

        clientConfig.setChannelHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                KryoSerializer serializer = new KryoSerializer(MQMessage.class);
                ch.pipeline().addLast(new MessageObjectDecoder(serializer));
                ch.pipeline().addLast(new MessageObjectEncoder(serializer));
                ch.pipeline().addLast(new MsgConsumerHandler());
            }
        });

        // 创建Broker Server's Client并启动
        nettyClient = new NettyClient(clientConfig);
        nettyClient.start();

        if (nettyClient.isStarted()) {
            LOGGER.info("transport connect cluster[{}] success .", clusterAddress);
        }
    }

    private class MsgConsumerHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            MsgSubscribe subscribe = new MsgSubscribe("MQTest", "MQTestGroup");
            MQMessage mqMessage = new MQMessage(MQSubscribe, ActiveMQConsumer, subscribe);
            ctx.writeAndFlush(mqMessage);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg.getClass() != MQMessage.class) {
                return;
            }

            MQMessage mqMessage = (MQMessage) msg;
            MQMsgType type = mqMessage.getMsgType();
            switch (type) {
                case MQSubscribeAck:
                    doMsgSubscribeAck(mqMessage);
                    break;
                case MQMsgResponse:
                    doMsgConsume(mqMessage);
                    break;
            }
        }


        private void doMsgSubscribeAck(MQMessage mqMessage) {
            MsgSubscribeAck subscribeAck = (MsgSubscribeAck) mqMessage.getMsg();
            LOGGER.info("consume subscribe topic : {}, status : {}", subscribeAck.getTopic(), subscribeAck.getStatus());
        }

        private void doMsgConsume(MQMessage mqMessage) {
            MsgResponse response = (MsgResponse) mqMessage.getMsg();
            Long start = response.getStart();
            Long end = response.getEnd();
            List<String> msgList = response.getMsgList().subList(start.intValue(), end.intValue());
            LOGGER.info("consume msg : {}", msgList);
        }
    }

    public static void main(String[] args) {
        String clusterAddress = "127.0.0.1:6712";
        MQConsumer consumer = new MQConsumer(clusterAddress);
        consumer.start();
    }
}
