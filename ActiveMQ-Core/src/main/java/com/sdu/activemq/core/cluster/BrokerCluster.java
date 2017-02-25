package com.sdu.activemq.core.cluster;

import com.google.common.collect.Maps;
import com.sdu.activemq.core.MQConfig;
import com.sdu.activemq.core.broker.BrokerMessageHandler;
import com.sdu.activemq.core.broker.client.BrokerTransport;
import com.sdu.activemq.core.broker.client.BrokerTransportPool;
import com.sdu.activemq.core.zk.ZkClientContext;
import com.sdu.activemq.core.zk.ZkConfig;
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
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static com.sdu.activemq.msg.MQMsgSource.MQCluster;
import static com.sdu.activemq.msg.MQMsgType.MQMessageRequest;
import static com.sdu.activemq.msg.MQMsgType.MQSubscribeAck;
import static com.sdu.activemq.utils.Const.ZK_BROKER_PATH;
import static com.sdu.activemq.utils.Const.ZK_TOPIC_PATH;

/**
 * BrokerCluster职责:
 *
 *  1: 路由MQ消息
 *
 *  2: 探测Broker存活
 *
 *
 * @author hanhan.zhang
 * */
public class BrokerCluster implements Cluster {

    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerCluster.class);

    // Broker链接表[每个Broker拥有一个客户端连接池]
    private ConcurrentHashMap<BrokerNode, BrokerTransportPool> connectors;

    // 记录Topic消费序号[key = 消息主题, value = [key = 消费组名, value = 消费位置]]
    private ConcurrentHashMap<String, Map<String, AtomicLong>> topicConsumeRecord;

    // 消息消费推送路由表[key = 消息主题, value = [key = 消费组, value = 客户端连接集合]]
    private ConcurrentHashMap<String, Map<String, List<Channel>>> topicConsumeClientRecord;

    // 记录'Producer'路由表[key = Producer服务地址, value = Netty Channel]
    private ConcurrentHashMap<String, Channel> producerRoute;

    // Zk
    private ZkClientContext zkClientContext;

    private ZkConfig zkConfig;

    private ClusterConfig clusterConfig;

    private Random loadRandom;

    private NettyServer nettyServer;

    public BrokerCluster(MQConfig mqConfig) {
        zkConfig = new ZkConfig(mqConfig);
        clusterConfig = new ClusterConfig(mqConfig);
        connectors = new ConcurrentHashMap<>();
        topicConsumeRecord = new ConcurrentHashMap<>();
        topicConsumeClientRecord = new ConcurrentHashMap<>();
        producerRoute = new ConcurrentHashMap<>();
        loadRandom = new Random();
    }

    @Override
    public void start() throws Exception {
        zkClientContext = new ZkClientContext(zkConfig);
        zkClientContext.start();

        // Broker Server上线/下线监控
        zkClientContext.addPathListener(ZK_BROKER_PATH, true, new BrokerPathChildrenCacheListener());
        // Topic Message节点监控
        zkClientContext.addPathListener(ZK_TOPIC_PATH, true, new TopicMessagePathChildrenCacheListener());

        // 启动Server
        startClusterServer();
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
                ch.pipeline().addLast(new ClusterMsgHandler());
            }
        });

        nettyServer = new NettyServer(nettyServerConfig);
        nettyServer.start();

        nettyServer.blockUntilStarted(2);

        if (!nettyServer.isServing()) {
            throw new IllegalStateException("broker server start failed.");
        }

        LOGGER.info("broker cluster start success, bind address : {}", Utils.socketAddressCastString(nettyServer.getSocketAddress()));

        // JVM关闭钩子
        Runtime.getRuntime().addShutdownHook(new ShutdownHook());
    }


    @Override
    public void destroy() throws Exception {
        zkClientContext.destroy();
    }

    // 消息存储负载均衡
    private BrokerNode loadBalance() {
        ArrayList<BrokerNode> brokerNodes = new ArrayList<>();
        Enumeration<BrokerNode> it = connectors.keys();
        while (it.hasMoreElements()) {
            brokerNodes.add(it.nextElement());
        }

        return brokerNodes.get(loadRandom.nextInt(brokerNodes.size()));
    }

    /**
     * Cluster职责:
     *
     *  1: 监听'Consumer'主题消息订阅
     *
     *  2: 监听'Producer'主题消息存储
     *
     * */
    private class ClusterMsgHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg.getClass() == MQMessage.class) {
                MQMessage mqMessage = (MQMessage) msg;
                MQMsgType type = mqMessage.getMsgType();
                switch (type) {
                    case MQSubscribe:
                        doMsgSubscribe(ctx, mqMessage);
                        break;
                    case MQMessageStore:
                        doMsgStore(ctx, mqMessage);
                        break;
                }
            }
            super.channelRead(ctx, msg);
        }

        /**
         * 'Consumer'订阅主题消息
         * */
        private void doMsgSubscribe(ChannelHandlerContext ctx, MQMessage mqMessage) {
            MsgSubscribe subscribe = (MsgSubscribe) mqMessage.getMsg();
            Channel channel = ctx.channel();
            Map<String, List<Channel>> consumeClientRecord = topicConsumeClientRecord.get(subscribe.getTopic());
            if (consumeClientRecord == null) {
                consumeClientRecord = new ConcurrentHashMap<>();
            }
            List<Channel> clients = consumeClientRecord.get(subscribe.getConsumerGroup());
            if (clients == null) {
                clients = new CopyOnWriteArrayList<>();
            }
            clients.add(channel);
            consumeClientRecord.put(subscribe.getConsumerGroup(), clients);
            topicConsumeClientRecord.put(subscribe.getTopic(), consumeClientRecord);

            // 初始化消费位置记录
            ConcurrentHashMap<String, AtomicLong> consumeRecord = new ConcurrentHashMap<>();
            consumeRecord.put(subscribe.getConsumerGroup(), new AtomicLong(0L));
            topicConsumeRecord.put(subscribe.getTopic(), consumeRecord);

            // 响应消费
            MsgSubscribeAck subscribeAck = new MsgSubscribeAck(subscribe.getTopic(), MsgAckStatus.SUCCESS);
            MQMessage msg = new MQMessage(MQSubscribeAck, MQCluster, subscribeAck);
            ctx.writeAndFlush(msg);
        }

        /**
         * 主题消息存储:
         *
         *  1: 保存'Producer'路由表[尚未做删除]
         *
         *  2: 'Broker'路由选择并转存主题消息
         *
         * */
        private void doMsgStore(ChannelHandlerContext ctx, MQMessage mqMessage) throws Exception {
            Channel channel = ctx.channel();

            // 保存路由表消息
            String producerAddress = Utils.socketAddressCastString((InetSocketAddress) channel.remoteAddress());
            producerRoute.put(producerAddress, channel);

            MsgContent msgContent = (MsgContent) mqMessage.getMsg();

            // 路由'Broker'并转存主题消息
            String topicPath = ZkUtils.brokerTopicNode(msgContent.getTopic());
            List<String> brokerServerList = zkClientContext.getChildNode(topicPath);
            BrokerNode brokerNode;
            if (brokerServerList == null || brokerServerList.isEmpty()) {
                brokerNode = loadBalance();
            } else {
                // 暂且只存储在一个Broker服务
                String brokerId = brokerServerList.get(0);
                String brokerZkPath = ZkUtils.brokerTopicNode(msgContent.getTopic(), brokerId);
                byte []dataByte = zkClientContext.getNodeData(brokerZkPath);
                String data = new String(dataByte);
                if (Strings.isNotEmpty(data)) {
                    BrokerMessageHandler.TopicNodeData topicNodeData = GsonUtils.fromJson(data, BrokerMessageHandler.TopicNodeData.class);
                    brokerNode = new BrokerNode(topicNodeData.getBrokerId(), Utils.stringCastSocketAddress(topicNodeData.getBrokerServer(), ":"));
                } else {
                    brokerNode = loadBalance();
                }
            }

            // 转存主题消息
            BrokerTransportPool pool = connectors.get(brokerNode);
            BrokerTransport transport = pool.borrowObject();
            transport.writeAndFlush(mqMessage);
            pool.returnObject(transport);
        }
    }

    /**
     * Broker MQ消息处理[目前单节点, 存在问题: 处理各种网络连接/高可用]:
     *
     *  1: 对'Producer'的主题消息存储确认
     *
     *  2: 主题消息发生变化, 请求变更的主题消息并推送到'Consumer'消费端
     *
     *  Note:
     *
     *    BrokerMsgHandler被过个BrokerTransport恭喜, 需添加@Sharable标签
     * */
    @ChannelHandler.Sharable
    private class BrokerMsgHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg.getClass() == MQMessage.class) {
                MQMessage mqMessage = (MQMessage) msg;
                MQMsgType type = mqMessage.getMsgType();
                switch (type) {
                    case MQMessageStoreAck:
                        doMsgStoreAck(mqMessage);
                        break;
                    case MQMessageResponse:
                        doMsgResponse(mqMessage);
                        break;
                }
            }
            super.channelRead(ctx, msg);
        }

        /**
         * 主题消息存储确认
         * */
        private void doMsgStoreAck(MQMessage mqMessage) {
            MsgAckImpl ackMessage = (MsgAckImpl) mqMessage.getMsg();
            String address = ackMessage.getProducerAddress();
            Channel channel = producerRoute.get(address);
            channel.writeAndFlush(mqMessage);
        }

        private void doMsgResponse(MQMessage msg) throws Exception {
            MsgResponse response = (MsgResponse) msg.getMsg();
            String topic = response.getTopic();

            // 消费位置
            Map<String, AtomicLong> consumeRecord = topicConsumeRecord.get(topic);

            // 推送'Consumer'消费端
            Map<String, List<Channel>> consumeClient = topicConsumeClientRecord.get(topic);
            if (consumeClient == null) {
                return;
            }

            consumeClient.forEach((group, channels) -> {
                Collections.shuffle(channels);
                Channel channel = channels.get(0);
                // 消费组消费位置
                AtomicLong position = consumeRecord.get(group);
                long start = position.get();
                position.set(start + response.getEnd() - response.getStart());
                response.setStart(start);
                channel.writeAndFlush(msg);
            });
        }
    }

    /**
     * Topic Message节点监控
     *
     * @apiNote
     *
     *  1: Topic下有新消息, 通知Topic下的消费者
     *
     * */
    private class TopicMessagePathChildrenCacheListener implements PathChildrenCacheListener {

        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
            PathChildrenCacheEvent.Type type = event.getType();
            ChildData childData = event.getData();
            switch (type) {
                case CHILD_ADDED:
                    topicChangedAndRequest(childData);
                    break;
                case CHILD_UPDATED:
                    topicChangedAndRequest(childData);
                    break;
            }
        }

        // Topic消息发生变化, 向Broker发送消息请求
        // Note:
        //  存在问题: 消费组消费位置不统一, 暂时消费组中消费最小位置[浪费网络资源]
        private void topicChangedAndRequest(ChildData childData) throws Exception {
            if (childData == null) {
                return;
            }
            String data = new String(childData.getData());
            if (Strings.isNotEmpty(data)) {
                BrokerMessageHandler.TopicNodeData topicNodeData = GsonUtils.fromJson(data, BrokerMessageHandler.TopicNodeData.class);
                // 向Broker发送数据请求
                InetSocketAddress socketAddress = Utils.stringCastSocketAddress(topicNodeData.getBrokerServer(), ":");
                BrokerNode brokerNode = new BrokerNode(topicNodeData.getBrokerId(), socketAddress);
                BrokerTransportPool pool = connectors.get(brokerNode);
                BrokerTransport transport = pool.borrowObject();
                //
                long startSequence = getTopicConsumeMinSequence(topicNodeData.getTopic(), topicConsumeRecord);
                long endSequence = topicNodeData.getCurrentMsgSequence();
                MsgRequest request = new MsgRequest(topicNodeData.getTopic(), startSequence, endSequence);
                MQMessage mqMessage = new MQMessage(MQMessageRequest, MQCluster, request);
                transport.writeAndFlush(mqMessage);
                pool.returnObject(transport);
            }
        }

        private long getTopicConsumeMinSequence(String topic, Map<String, Map<String, AtomicLong>> topicConsumeRecord) {
            if (topicConsumeRecord == null) {
                return 0L;
            }
            Map<String, AtomicLong> consumeRecord = topicConsumeRecord.get(topic);
            if (consumeRecord == null) {
                return 0L;
            }
            return consumeRecord.values().stream().min((o1, o2) -> (int) (o1.get() - o2.get())).get().get();
        }
    }

    /**
     * Broker Server上线/下线监控
     * */
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
                case CHILD_ADDED:
                    updateBroker(childData);
                    break;
                case CHILD_UPDATED:
                    updateBroker(childData);
                    break;
                case CHILD_REMOVED:
                    deleteBroker(childData);
                    break;
            }
        }

        /**
         * Broker服务节点更新
         * */
        private void updateBroker(ChildData childData) throws Exception {
            String path = childData.getPath();
            // Broker唯一标识
            String UUID = path.substring(ZK_BROKER_PATH.length() + 1);
            // Broker服务地址
            String brokerAddress = new String(childData.getData());
            InetSocketAddress socketAddress = Utils.stringCastSocketAddress(brokerAddress, ":");

            LOGGER.info("Broker server node[{}] online .", brokerAddress);

            BrokerNode node = new BrokerNode(UUID, socketAddress);
            BrokerTransportPool pool = connectors.get(node);
            if (pool == null) {
                pool = new BrokerTransportPool(brokerAddress, zkConfig.getMqConfig(), new BrokerMsgHandler());
                connectors.put(node, pool);
            } else {
                String address = pool.getBrokerAddress();
                if (!address.equals(brokerAddress)) {
                    // Broker Server服务地址 ,l发生变化, 需重新创建连接
                    pool.destroy();
                    pool = new BrokerTransportPool(brokerAddress, zkConfig.getMqConfig(), new BrokerMsgHandler());
                    connectors.put(node, pool);
                }
            }
        }

        /**
         * Broker服务节点下线
         * */
        private void deleteBroker(ChildData childData) {
            String path = childData.getPath();
            // Broker唯一标识
            String UUID = path.substring(ZK_BROKER_PATH.length() + 1);
            // Broker服务地址
            String brokerAddress = new String(childData.getData());

            LOGGER.info("broker server node[{}] offline .", brokerAddress);

            InetSocketAddress socketAddress = Utils.stringCastSocketAddress(brokerAddress, ":");
            BrokerNode node = new BrokerNode(UUID, socketAddress);
            connectors.remove(node);
        }
    }

    private class ShutdownHook extends Thread {
        @Override
        public void run() {
            try {
                BrokerCluster.this.destroy();
            } catch (Exception e) {
                // ignore
            }
        }
    }
}
