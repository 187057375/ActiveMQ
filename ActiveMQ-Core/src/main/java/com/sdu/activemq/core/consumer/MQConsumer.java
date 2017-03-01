package com.sdu.activemq.core.consumer;

import com.google.common.collect.Maps;
import com.sdu.activemq.core.MQConfig;
import com.sdu.activemq.core.transport.DataTransport;
import com.sdu.activemq.core.transport.TransportPool;
import com.sdu.activemq.core.zk.ZkClientContext;
import com.sdu.activemq.core.zk.ZkConfig;
import com.sdu.activemq.core.zk.node.ZkMsgDataNode;
import com.sdu.activemq.msg.*;
import com.sdu.activemq.network.client.NettyClient;
import com.sdu.activemq.network.client.NettyClientConfig;
import com.sdu.activemq.network.serialize.MessageObjectDecoder;
import com.sdu.activemq.network.serialize.MessageObjectEncoder;
import com.sdu.activemq.network.serialize.kryo.KryoSerializer;
import com.sdu.activemq.util.Utils;
import com.sdu.activemq.utils.GsonUtils;
import com.sdu.activemq.utils.ZkUtils;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static com.sdu.activemq.msg.MQMsgSource.MQConsumer;
import static com.sdu.activemq.msg.MQMsgSource.MQCluster;
import static com.sdu.activemq.msg.MQMsgType.*;
import static org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.CHILD_UPDATED;

/**
 *
 * MQ Consumer
 *
 * 1: 尚未做zk节点更新
 *
 * @author hanhan.zhang
 * */
public class MQConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MQConsumer.class);

    private String clusterAddress;

    private String topic;

    private String topicGroup;

    private NettyClient nettyClient;

    private MQConfig mqConfig;

    private Map<String, TransportPool> transportPools;

    // 任务线程
    private ExecutorService executorService;

    private ZkClientContext zkClientContext;

    private Map<String, AtomicLong> brokerSequence = Maps.newConcurrentMap();

    public MQConsumer(String clusterAddress, String topic, String topicGroup, MQConfig mqConfig) {
        this.clusterAddress = clusterAddress;
        this.topic = topic;
        this.topicGroup = topicGroup;
        this.mqConfig = mqConfig;
        this.transportPools = Maps.newConcurrentMap();
        this.executorService = Executors.newSingleThreadExecutor();
        this.zkClientContext = new ZkClientContext(new ZkConfig(mqConfig));
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
                ch.pipeline().addLast(new ConsumeMessageHandler());
            }
        });

        nettyClient = new NettyClient(clientConfig);
        nettyClient.start();

        if (nettyClient.isStarted()) {
            LOGGER.info("transport connect cluster[{}] success .", clusterAddress);
        }

        // 启动Zk
        zkClientContext.start();
    }

    private class ConsumeMessageHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            TopicStoreRequest request = new TopicStoreRequest(topic, 0);
            MQMessage mqMessage = new MQMessage(MQTopicStoreRequest, MQConsumer, request);
            ctx.writeAndFlush(mqMessage);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg.getClass() != MQMessage.class) {
                ctx.fireChannelRead(msg);
                return;
            }

            MQMessage mqMessage = (MQMessage) msg;
            if (mqMessage.getMsgType() == MQTopicStoreResponse) {
                TopicStoreResponse response = (TopicStoreResponse) mqMessage.getMsg();
                executorService.submit(() -> {
                    Set<String> brokerAddress = response.getBrokerAddress();
                    if (brokerAddress != null && brokerAddress.size() > 0) {
                        for (String address : brokerAddress) {
                            TransportPool pool = new TransportPool(address, mqConfig, new MessageReceiveHandler());
                            transportPools.put(address, pool);

                            // 添加Broker Server数据节点变化
                            String zkMsgDataPath = ZkUtils.zkMsgDataNode(response.getTopic(), address);
                            try {
                                zkClientContext.addPathListener(zkMsgDataPath, true, new MessageDataPathChildrenCacheListener());
                            } catch (Exception e) {
                                LOGGER.error("zk add node path[{}] listener exception", zkMsgDataPath, e);
                            }
                        }
                    }
                });
            }
        }
    }

    @ChannelHandler.Sharable
    private class MessageReceiveHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg.getClass() != MQMessage.class) {
                ctx.fireChannelRead(msg);
                return;
            }

            MQMessage mqMessage = (MQMessage) msg;
            if (mqMessage.getMsgType() == MQConsumeResponse) {
                MsgConsumeResponse response = (MsgConsumeResponse) mqMessage.getMsg();
                LOGGER.info("consume message : {}", response.getMessages());
            }

        }
    }

    private class MessageDataPathChildrenCacheListener implements PathChildrenCacheListener {

        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
            if (event.getType() == CHILD_UPDATED) {
                topicChangedAndRequest(event.getData());
            }
        }

        private void topicChangedAndRequest(ChildData childData) throws Exception {
            if (childData == null) {
                return;
            }
            String data = new String(childData.getData());
            if (Strings.isNotEmpty(data)) {
                ZkMsgDataNode topicNodeData = GsonUtils.fromJson(data, ZkMsgDataNode.class);

                if (topicNodeData.getCurrentMsgSequence() <= 0) {
                    return;
                }

                String brokerServer = topicNodeData.getBrokerServer();

                TransportPool pool = transportPools.get(brokerServer);
                if (pool == null) {
                    throw new IllegalStateException("can't find broker server {} connect pool");
                }

                AtomicLong sequence = brokerSequence.get(brokerServer);
                if (sequence == null) {
                    sequence = new AtomicLong(0);
                    brokerSequence.put(brokerServer, sequence);
                }

                if (sequence.get() >= topicNodeData.getCurrentMsgSequence()) {
                    return;
                }

                sequence.set(topicNodeData.getCurrentMsgSequence());

                LOGGER.info("consume broker[{}] message from {} to {} .", brokerServer, sequence.get(), topicNodeData.getCurrentMsgSequence());

                // Broker发送消息请求
                MsgConsumeRequest request = new MsgConsumeRequest(topicNodeData.getTopic(), sequence.get(), topicNodeData.getCurrentMsgSequence());
                MQMessage mqMessage = new MQMessage(MQConsumeRequest, MQCluster, request);

                DataTransport transport = pool.borrowObject();
                transport.writeAndFlush(mqMessage);
                pool.returnObject(transport);
            }
        }
    }
}
