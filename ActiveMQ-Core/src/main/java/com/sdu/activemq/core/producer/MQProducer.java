package com.sdu.activemq.core.producer;

import com.google.common.collect.Lists;
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
import com.sdu.activemq.utils.GsonUtils;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.sdu.activemq.msg.MQMsgSource.MQProducer;
import static com.sdu.activemq.msg.MQMsgType.MQBrokerAllocateRequest;
import static com.sdu.activemq.msg.MQMsgType.MQMsgStore;
import static com.sdu.activemq.msg.MQMsgType.MQMsgStoreAck;

/**
 * MQ消息生产者职责:
 *
 * 1: 请求Cluster为主题Topic分配Broker
 *
 * 2: 对分配的Broker的主题Topic创建Broker连接池
 *
 * MQ消息发送有两种方式:
 *
 * 1: 立即发送
 *
 * 2: 批量发送[暂时有问题]
 *
 * @author hanhan.zhang
 * */
public class MQProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MQProducer.class);

    // 集群服务地址
    private String clusterAddress;

    // 生产消息的主题
    private String topic;

    private MQConfig mqConfig;

    // 消息队列
    private DisruptorQueue queue;

    private NettyClient nettyClient;

    // Broker连接池[key = broker地址, value = 连接池]
    private Map<String, TransportPool> brokerConnectPool;

    // 任务线程
    private ExecutorService executorService;

    private MessageSenderHandler senderHandler;

    private AtomicBoolean started = new AtomicBoolean(false);

    public MQProducer(String topic, MQConfig mqConfig) {
        this.topic = topic;
        this.clusterAddress = mqConfig.getString("cluster.address", "127.0.0.1:6712");
        this.mqConfig = mqConfig;
        this.senderHandler = new MessageSenderHandler();
        this.queue = new DisruptorQueue(ProducerType.SINGLE, this.senderHandler, mqConfig);
        this.brokerConnectPool = Maps.newConcurrentMap();
        this.executorService = Executors.newSingleThreadExecutor();
    }

    public void start() throws Exception {
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

        nettyClient.blockUntilStarted(2);

        if (nettyClient.isStarted()) {
            LOGGER.info("producer connect cluster[{}] success .", clusterAddress);
        }

        Runtime.getRuntime().addShutdownHook(new ShutdownHook());
    }


    public void sendMsg(MQMessage mqMessage, boolean rightNow) throws Exception {
        if (!started.get()) {
            throw new IllegalStateException("producer is not started");
        }
        if (mqMessage.getMsgType() != MQMsgStore) {
            throw new IllegalArgumentException("message type must be MQMsgStore");
        }
        MsgStoreRequest content = (MsgStoreRequest) mqMessage.getMsg();
        if (content.getTopic() == null || !content.getTopic().equals(this.topic)) {
            throw new IllegalArgumentException("message topic must be " + topic);
        }

        if (rightNow) {
            senderHandler.handle(mqMessage);
        } else {
            queue.publish(mqMessage);
        }
    }

    private void close() throws Exception {
        started.set(false);
        if (nettyClient != null && nettyClient.isStarted()) {
            nettyClient.stop(10, TimeUnit.SECONDS);
        }

        if (executorService != null) {
            executorService.shutdown();
        }

        if (brokerConnectPool != null && brokerConnectPool.size() > 0) {
            for (Map.Entry<String, TransportPool> entry : brokerConnectPool.entrySet()) {
                entry.getValue().destroy();
            }
        }

    }

    private class MessageSenderHandler implements MessageHandler {
        @Override
        public void handle(Object msg) throws Exception {
            if (msg.getClass() != MQMessage.class) {
                return;
            }

            MQMessage mqMessage = (MQMessage) msg;
            MsgStoreRequest content = (MsgStoreRequest) mqMessage.getMsg();

            // 负责均衡[未做]
            TransportPool pool = Lists.newArrayList(brokerConnectPool.values()).get(0);

            if (pool == null) {
                throw new IllegalStateException("can't find topic " + content.getTopic() + " store broker");
            }

            DataTransport transport = pool.borrowObject();
            transport.writeAndFlush(mqMessage);
            pool.returnObject(transport);
        }

        @Override
        public void handle(List<Object> batchMsg) throws Exception {
            if (batchMsg == null || batchMsg.isEmpty()) {
                return;
            }


            for (Object msg : batchMsg) {
                if (msg.getClass() != MQMessage.class) {
                    continue;
                }
                handle(msg);
            }

        }
    }


    private class ProducerMessageHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            BrokerAllocateRequest request = new BrokerAllocateRequest(topic, 0);
            MQMessage mqMessage = new MQMessage(MQBrokerAllocateRequest, MQProducer, request);
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
                brokerConnectPool.put(response.getBrokerAddress(), pool);
                started.set(true);
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
                LOGGER.info("message store ack : {} ", GsonUtils.toPrettyJson(msgAck));
            }
        }
    }

    private class ShutdownHook extends Thread {
        @Override
        public void run() {
            try {
                MQProducer.this.close();
            } catch (Exception e) {
                LOGGER.error("Producer close occur exception", e);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        MQProducer producer = new MQProducer("mq.test", new MQConfig("producer.cfg"));
        producer.start();

        while (true) {
            String msg = UUID.randomUUID().toString();
            MsgStoreRequest msgStoreRequest = new MsgStoreRequest("mq.test", "", msg, System.currentTimeMillis());
            MQMessage mqMessage = new MQMessage(MQMsgStore, MQProducer, msgStoreRequest);
            producer.sendMsg(mqMessage, true);
            TimeUnit.SECONDS.sleep(1);
        }

    }
}
