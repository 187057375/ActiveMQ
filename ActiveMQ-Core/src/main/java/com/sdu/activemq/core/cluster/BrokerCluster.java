package com.sdu.activemq.core.cluster;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sdu.activemq.core.MQConfig;
import com.sdu.activemq.core.zk.ZkClientContext;
import com.sdu.activemq.core.zk.ZkConfig;
import com.sdu.activemq.core.zk.node.ZkBrokerNode;
import com.sdu.activemq.core.zk.node.ZkConsumeMetaNode;
import com.sdu.activemq.core.zk.node.ZkMsgTopicNode;
import com.sdu.activemq.msg.*;
import com.sdu.activemq.network.serialize.MessageObjectDecoder;
import com.sdu.activemq.network.serialize.MessageObjectEncoder;
import com.sdu.activemq.network.serialize.kryo.KryoSerializer;
import com.sdu.activemq.network.server.NettyServer;
import com.sdu.activemq.network.server.NettyServerConfig;
import com.sdu.activemq.utils.GsonUtils;
import com.sdu.activemq.utils.Utils;
import com.sdu.activemq.utils.ZkUtils;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannel;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.logging.log4j.util.Strings;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.sdu.activemq.msg.MQMsgSource.MQCluster;
import static com.sdu.activemq.msg.MQMsgType.MQBrokerAllocateResponse;
import static com.sdu.activemq.msg.MQMsgType.MQTopicStoreResponse;
import static com.sdu.activemq.utils.Const.*;

/**
 * Broker Cluster职责:
 *
 *  1: 维护Broker Server节点信息, 用于负载均衡存储消息
 *
 *  2: 维护主题消息存储节点映射
 *
 *  3: 维护主题消息客户端定义信息
 *
 * Broker Cluster处理的网络请求:
 *
 * 1: Producer申请主题存储节点
 *
 * 2: Consumer申请主题消息的Broker的存储节点
 *
 * Broker Cluster监控Zk两种节点
 *
 * 1: Broker Server节点
 *
 *   更新Server节点信息[/activeMQ/broker/host:port]
 *
 * 2: Consume Subscribe节点[/activeMQ/consume]
 *
 *  更新主题消息客户端定义信息
 *
 * @author hanhan.zhang
 * */
public class BrokerCluster {

    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerCluster.class);

    // Broker节点
    private Set<EndPoint> endPoints;

    // 主题订阅[key = 主题, value = [key = 消费组, value = 消费者节点信息]]
    private Map<String, Map<String, Set<EndPoint>>> topicSubscribe;

    // 主题存储[key = 主题, value = broker存储节点]
    private Map<String, Set<EndPoint>> topicStore;

    private ZkClientContext zkClientContext;

    private NettyServer nettyServer;

    private MQConfig mqConfig;

    private ClusterConfig clusterConfig;

    public BrokerCluster(MQConfig mqConfig) {
        this.mqConfig = mqConfig;
        this.clusterConfig = new ClusterConfig(mqConfig);
        this.endPoints = Sets.newConcurrentHashSet();
        this.topicSubscribe = Maps.newConcurrentMap();
        this.topicStore = Maps.newConcurrentMap();
    }

    public void start() throws Exception {
        connectZk(mqConfig);
        startClusterServer();
        // JVM关闭钩子
        Runtime.getRuntime().addShutdownHook(new ShutdownHook());
    }

    private void updateTopicStore(String topic, EndPoint point) {
        Set<EndPoint> endPoints = topicStore.get(topic);
        if (endPoints == null) {
            endPoints = Sets.newConcurrentHashSet();
            topicStore.put(topic, endPoints);
        }
        endPoints.add(point);
    }

    private boolean isBrokerActive(String brokerAddress, String  brokerId) {
        EndPoint point = new EndPoint(brokerAddress, brokerId);
        return endPoints.contains(point);
    }

    private EndPoint getBrokerNode(String topic) throws Exception {
        byte []dataByte = zkClientContext.getNodeData(ZkUtils.zkTopicMetaNode(topic));
        if (dataByte != null) {
            String data = new String(dataByte);
            if (Strings.isNotEmpty(data)) {
                ZkMsgTopicNode storeNode = GsonUtils.fromJson(data, ZkMsgTopicNode.class);
                boolean isActive = isBrokerActive(storeNode.getBrokerAddress(), storeNode.getBrokerId());
                if (isActive) {
                    EndPoint endPoint = new EndPoint(storeNode.getBrokerId(), storeNode.getBrokerAddress());
                    updateTopicStore(topic, endPoint);
                    return endPoint;
                }

                // 删除旧数据
                zkClientContext.deleteNode(ZkUtils.zkTopicMetaNode(topic));
            }
        }

        // 随机选择一个节点
        List<EndPoint> shuffleEndPoints = Lists.newLinkedList(this.endPoints);
        Collections.shuffle(shuffleEndPoints);
        EndPoint endPoint = shuffleEndPoints.get(0);
        updateTopicStore(topic, endPoint);

        // 创建ZK Store节点
        ZkMsgTopicNode storeNode = new ZkMsgTopicNode(endPoint.getNodeAddress(), endPoint.getPointID(), topic);
        zkClientContext.createNode(ZkUtils.zkTopicMetaNode(topic), GsonUtils.toJson(storeNode), CreateMode.PERSISTENT);

        return endPoint;
    }

    private Set<String > getTopicStoreNode(String topic) {
        Set<EndPoint> endPoints = topicStore.get(topic);
        if (endPoints == null) {
            return Collections.emptySet();
        }
        return endPoints.stream().map(EndPoint::getNodeAddress).collect(Collectors.toSet());
    }

    public void destroy() throws Exception {
        if (zkClientContext != null && zkClientContext.isServing()) {
            zkClientContext.destroy();
        }
        if (nettyServer != null && nettyServer.isServing()) {
            nettyServer.stop(10, TimeUnit.SECONDS);
        }
    }

    private void startClusterServer() throws Exception {

        // Netty Serve配置
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setBossThreadFactory(Utils.buildThreadFactory("cluster-accept-thread-%d"));
        nettyServerConfig.setWorkerThreadFactory(Utils.buildThreadFactory("cluster-socket-thread-%d"));
        nettyServerConfig.setEPoll(false);
        nettyServerConfig.setSocketThreads(clusterConfig.getClusterSocketThread());
        nettyServerConfig.setHost(clusterConfig.getClusterAddressHost());
        nettyServerConfig.setPort(clusterConfig.getClusterAddressPort());
        // Netty Server Socket配置
        Map<ChannelOption, Object> options = Maps.newHashMap();
        options.put(ChannelOption.SO_BACKLOG, 1024);
        options.put(ChannelOption.SO_REUSEADDR, true);
        options.put(ChannelOption.SO_KEEPALIVE, false);
        options.put(ChannelOption.SO_SNDBUF, clusterConfig.getClusterSocketSndBuf());
        options.put(ChannelOption.SO_RCVBUF, clusterConfig.getClusterSocketRcvBuf());
        options.put(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        nettyServerConfig.setOptions(options);

        Map<ChannelOption, Object> childOptions = Maps.newHashMap();
        childOptions.put(ChannelOption.TCP_NODELAY, true);
        childOptions.put(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        nettyServerConfig.setChildOptions(childOptions);
        nettyServerConfig.setChildChannelHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                KryoSerializer serializer = new KryoSerializer(MQMessage.class);
                // 设置Socket数据通信编码
                ch.pipeline().addLast(new MessageObjectDecoder(serializer));
                ch.pipeline().addLast(new MessageObjectEncoder(serializer));
                ch.pipeline().addLast(new ClusterMessageHandler());
            }
        });

        nettyServer = new NettyServer(nettyServerConfig);
        nettyServer.start();

        nettyServer.blockUntilStarted(2);

        if (!nettyServer.isServing()) {
            throw new IllegalStateException("broker server start failed.");
        }

        LOGGER.info("broker cluster start success, bind address : {}", Utils.socketAddressCastString(nettyServer.getSocketAddress()));
    }

    /**
     * 连接ZK并监听节点[/activeMQ/broker/brokerId]变化
     * */
    private void connectZk(MQConfig mqConfig) throws Exception {
        zkClientContext = new ZkClientContext(new ZkConfig(mqConfig));
        zkClientContext.start();

        // Broker上线/下线监控
        zkClientContext.addPathListener(ZK_BROKER_PATH, true, new BrokerPathChildrenCacheListener());
        // 监控消息订阅节点变化
        zkClientContext.addSubAllPathListener(ZK_MSG_CONSUME_PATH, new ConsumeMetaPathChildrenCacheListener());

    }


    private class BrokerPathChildrenCacheListener implements PathChildrenCacheListener {

        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
            PathChildrenCacheEvent.Type type = event.getType();
            ChildData childData = event.getData();
            if (childData == null) {
                return;
            }
            // 处理节点变化
            switch (type) {
                case CHILD_UPDATED:
                    updateBroker(childData);
                    break;
                case CHILD_ADDED:
                    updateBroker(childData);
                    break;
                case CHILD_REMOVED:
                    deleteBroker(childData);
                    break;
            }
        }

        // Broker服务节点更新
        private void updateBroker(ChildData childData) throws Exception {
            if (childData == null || childData.getData() == null || childData.getData().length == 0) {
                return;
            }

            ZkBrokerNode zkNodeData = GsonUtils.fromJson(new String(childData.getData()), ZkBrokerNode.class);

            LOGGER.info("broker server[{}] online and update broker cluster .", zkNodeData.getBrokerAddress());

            EndPoint node = new EndPoint(zkNodeData.getBrokerId(), zkNodeData.getBrokerAddress());

            endPoints.add(node);
        }

        // Broker服务节点下线
        private void deleteBroker(ChildData childData) {
            if (childData == null || childData.getData() == null || childData.getData().length == 0) {
                return;
            }

            ZkBrokerNode zkNodeData = GsonUtils.fromJson(new String(childData.getData()), ZkBrokerNode.class);

            String brokerAddress = zkNodeData.getBrokerAddress();

            LOGGER.info("broker server node[{}] offline and update broker cluster .", brokerAddress);

            EndPoint node = new EndPoint(zkNodeData.getBrokerId(), brokerAddress);

            endPoints.remove(node);
        }
    }

    private class ConsumeMetaPathChildrenCacheListener implements TreeCacheListener {

        @Override
        public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
            TreeCacheEvent.Type type = event.getType();
            switch (type) {
                case NODE_ADDED:
                    consumeNodeChanged(event.getData(), false);
                    break;
                case NODE_UPDATED:
                    consumeNodeChanged(event.getData(), false);
                    break;
                case NODE_REMOVED:
                    consumeNodeChanged(event.getData(), true);
                    break;
            }
        }

        private void consumeNodeChanged(ChildData childData, boolean off) throws Exception {
            if (childData == null) {
                return;
            }
            String data = new String(childData.getData());
            ZkConsumeMetaNode metaNode = GsonUtils.fromJson(data, ZkConsumeMetaNode.class);
            if (metaNode == null) {
                return;
            }

            String topic = metaNode.getTopic();
            String topicGroup = metaNode.getTopicGroup();
            EndPoint point = new EndPoint(metaNode.getConsumeAddress());
            Map<String, Set<EndPoint>> groupSubscribe = topicSubscribe.get(topic);
            if (groupSubscribe == null) {
                if (off) {
                    return;
                }
                groupSubscribe = Maps.newConcurrentMap();
                topicSubscribe.put(topic, groupSubscribe);
            }

            Set<EndPoint> consumePoints = groupSubscribe.get(topicGroup);
            if (consumePoints == null) {
                if (off) {
                    return;
                }
                consumePoints = Sets.newConcurrentHashSet();
                groupSubscribe.put(topicGroup, consumePoints);
            }

            if (off) {
                consumePoints.remove(point);
            } else {
                consumePoints.add(point);
            }
        }
    }

    private class ClusterMessageHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg.getClass() != MQMessage.class) {
                ctx.fireChannelRead(msg);
                return;
            }
            MQMessage mqMessage = (MQMessage) msg;
            MQMsgType type = mqMessage.getMsgType();
            switch (type) {
                case MQBrokerAllocateRequest:
                    doBrokerAllocate(ctx, mqMessage);
                    break;
                case MQTopicStoreRequest:
                    doTopicStoreAsk(ctx, mqMessage);
                    break;
            }

        }

        private void doBrokerAllocate(ChannelHandlerContext ctx, MQMessage mqMessage) throws Exception {
            BrokerAllocateRequest request = (BrokerAllocateRequest) mqMessage.getMsg();
            EndPoint endPoint = getBrokerNode(request.getTopic());

            // 响应客户端
            BrokerAllocateResponse response = new BrokerAllocateResponse(endPoint.getPointID(), endPoint.getNodeAddress());
            MQMessage msg = new MQMessage(MQBrokerAllocateResponse, MQCluster, response);
            ctx.writeAndFlush(msg);
        }

        private void doTopicStoreAsk(ChannelHandlerContext ctx, MQMessage mqMessage) throws Exception {
            TopicStoreRequest request = (TopicStoreRequest) mqMessage.getMsg();
            Set<String > endPoints = getTopicStoreNode(request.getTopic());

            // 响应客户端
            TopicStoreResponse response = new TopicStoreResponse(request.getTopic(), request.getPartition(), endPoints);
            MQMessage msg = new MQMessage(MQTopicStoreResponse, MQCluster, response);
            ctx.writeAndFlush(msg);

        }
    }

    private class ShutdownHook extends Thread {
        @Override
        public void run() {
            try {
                BrokerCluster.this.destroy();
            } catch (Exception e) {
                LOGGER.error("shutdown cluster server exception", e);
            }
        }
    }
}
