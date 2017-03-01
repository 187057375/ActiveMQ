package com.sdu.activemq.core.producer;

import com.google.common.collect.Maps;
import com.lmax.disruptor.dsl.ProducerType;
import com.sdu.activemq.core.MQConfig;
import com.sdu.activemq.core.disruptor.DisruptorQueue;
import com.sdu.activemq.core.disruptor.MessageHandler;
import com.sdu.activemq.core.transport.DataTransport;
import com.sdu.activemq.core.transport.TransportPool;
import com.sdu.activemq.msg.*;
import com.sdu.activemq.network.client.NettyClient;
import com.sdu.activemq.network.client.NettyClientConfig;
import com.sdu.activemq.network.serialize.MessageObjectDecoder;
import com.sdu.activemq.network.serialize.MessageObjectEncoder;
import com.sdu.activemq.network.serialize.kryo.KryoSerializer;
import com.sdu.activemq.util.Utils;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.sdu.activemq.msg.MQMsgSource.ActiveMQProducer;
import static com.sdu.activemq.msg.MQMsgType.MQBrokerAllocateRequest;
import static com.sdu.activemq.msg.MQMsgType.MQMsgStore;
import static com.sdu.activemq.msg.MQMsgType.MQMsgStoreAck;

/**
 * @author hanhan.zhang
 * */
public class MQProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MQProducer.class);

    private String clusterAddress;

    private String topic;

    private MQConfig mqConfig;

    // 消息队列
    private DisruptorQueue queue;

    private NettyClient nettyClient;

    private Map<String, TransportPool> brokerConnectPool;

    // 任务线程
    private ExecutorService executorService;

    public MQProducer(String topic, String clusterAddress, MQConfig mqConfig) {
        this.topic = topic;
        this.clusterAddress = clusterAddress;
        this.mqConfig = mqConfig;
        this.queue = new DisruptorQueue(ProducerType.SINGLE, new MessageSenderHandler(), mqConfig);
        this.brokerConnectPool = Maps.newConcurrentMap();
        this.executorService = Executors.newSingleThreadExecutor();
    }

    private void start() {
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
                ch.pipeline().addLast(new ProducerMessageHandler());
            }
        });

        // 创建Broker Server's Client并启动
        nettyClient = new NettyClient(clientConfig);
        nettyClient.start();

        if (nettyClient.isStarted()) {
            LOGGER.info("transport connect cluster[{}] success .", clusterAddress);
        }
    }


    public void sendMsg(MQMessage mqMessage) {
        if (mqMessage.getMsgType() != MQMsgStore) {
            throw new IllegalArgumentException("message type must be MQMsgStore");
        }
        MsgContent content = (MsgContent) mqMessage.getMsg();
        if (content.getTopic() == null || !content.getTopic().equals(this.topic)) {
            throw new IllegalArgumentException("message topic must be " + topic);
        }
        queue.publish(mqMessage);
    }

    public void close() throws Exception {
        if (nettyClient != null && nettyClient.isStarted()) {
            nettyClient.stop(10, TimeUnit.SECONDS);
        }
    }

    private class MessageSenderHandler implements MessageHandler {
        @Override
        public void handle(Object msg) throws Exception {
            if (msg.getClass() != MQMessage.class) {
                return;
            }

            MQMessage mqMessage = (MQMessage) msg;
            MsgContent content = (MsgContent) mqMessage.getMsg();

            TransportPool pool = brokerConnectPool.get(content.getTopic());
            if (pool == null) {
                throw new IllegalStateException("can't find topic " + content.getTopic() + " store broker");
            }

            DataTransport transport = pool.borrowObject();
            transport.writeAndFlush(mqMessage);
            pool.returnObject(transport);
        }
    }


    private class ProducerMessageHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            BrokerAllocateRequest request = new BrokerAllocateRequest(topic, 0);
            MQMessage mqMessage = new MQMessage(MQBrokerAllocateRequest, ActiveMQProducer, request);
            ctx.writeAndFlush(mqMessage);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg.getClass() != MQMessage.class) {
                ctx.fireChannelRead(msg);
                return;
            }
            MQMessage mqMessage = (MQMessage) msg;
            final BrokerAllocateResponse response = (BrokerAllocateResponse) mqMessage.getMsg();

            executorService.submit(() -> {
                TransportPool pool = new TransportPool(response.getBrokerAddress(), mqConfig, new MessageStoreHandler());
                brokerConnectPool.put(topic, pool);
            });
        }
    }


    @ChannelHandler.Sharable
    private class MessageStoreHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg.getClass() != MQMessage.class) {
                ctx.fireChannelRead(msg);
                return;
            }
            MQMessage mqMessage = (MQMessage) msg;
            MQMsgType type = mqMessage.getMsgType();
            if (type == MQMsgStoreAck) {
                MsgAckImpl msgAck = (MsgAckImpl) mqMessage.getMsg();
                LOGGER.info("message store ack : topic = {}, msgId = {}, brokerSequence = {}, status = {}",
                        msgAck.getTopic(), msgAck.getMsgId(), msgAck.getBrokerMsgSequence(), msgAck.getAckStatus());
            }
        }
    }
}
