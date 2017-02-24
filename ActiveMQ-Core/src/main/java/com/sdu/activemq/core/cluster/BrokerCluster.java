package com.sdu.activemq.core.cluster;

import com.sdu.activemq.core.MQConfig;
import com.sdu.activemq.core.broker.BrokerMessageHandler;
import com.sdu.activemq.core.broker.client.BrokerTransport;
import com.sdu.activemq.core.broker.client.BrokerTransportPool;
import com.sdu.activemq.core.zk.ZkClientContext;
import com.sdu.activemq.core.zk.ZkConfig;
import com.sdu.activemq.model.MQMessage;
import com.sdu.activemq.model.MQMsgSource;
import com.sdu.activemq.model.MQMsgType;
import com.sdu.activemq.model.msg.*;
import com.sdu.activemq.utils.GsonUtils;
import com.sdu.activemq.utils.Utils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.internal.ConcurrentSet;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.logging.log4j.util.Strings;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.sdu.activemq.model.MQMsgSource.MQCluster;
import static com.sdu.activemq.model.MQMsgType.MQMessageRequest;
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

    // Broker链接表[每个Broker拥有一个客户端连接池]
    private ConcurrentHashMap<BrokerNode, BrokerTransportPool> connectors;

    // 记录Topic消费序号[key = 消息主题, value = [key = 消费组名, value = 消费位置]]
    private ConcurrentHashMap<String, Map<String, AtomicLong>> topicConsumeRecord;

    // 消息消费推送路由表[key = 消息主题, value = [key = 消费组, value = 客户端连接集合]]
    private ConcurrentHashMap<String, Map<String, Set<Channel>>> topConsumeClientRecord;

    // 记录'Producer'路由表[key = Producer服务地址, value = Netty Channel]
    private ConcurrentHashMap<String, Channel> producerRoute;


    // Zk
    private ZkClientContext zkClientContext;

    private ZkConfig zkConfig;

    private Random loadRandom;

    public BrokerCluster(MQConfig mqConfig) {
        zkConfig = new ZkConfig(mqConfig);
        connectors = new ConcurrentHashMap<>();
        topicConsumeRecord = new ConcurrentHashMap<>();
        topConsumeClientRecord = new ConcurrentHashMap<>();
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
        zkClientContext.addPathListener(ZK_BROKER_PATH, true, new TopicMessagePathChildrenCacheListener());
    }

    @Override
    public BrokerTransport getConnector(MQMessage msg) {
        return null;
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
     * MQ消息处理[目前单节点, 存在问题: 处理各种网络连接/高可用]
     *
     * @apiNote
     *
     *  1: 路由'Producer'消息存储, 收到'Broker'确认则更新ZK节点并向生产者确认
     *
     *  2: 向'Broker'请求消息并转发给消费者
     *
     * */
    private class MsgProcessHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg.getClass() == MQMessage.class) {
                MQMessage mqMessage = (MQMessage) msg;
                MQMsgType type = mqMessage.getMsgType();
                switch (type) {
                    case MQSubscribe:
                        doMsgConsumer(ctx, mqMessage);
                        break;
                    case MQMessageStoreAck:
                        doMsgStoreAck(mqMessage);
                        break;
                    case MQMessageStore:
                        doMsgStore(ctx, mqMessage);
                        break;
                    case MQMessageResponse:
                        break;
                }
            }
            super.channelRead(ctx, msg);
        }

        //
        private void doMsgConsumer(ChannelHandlerContext ctx, MQMessage mqMessage) {
            MsgSubscribe request = (MsgSubscribe) mqMessage.getMsg();
            Channel channel = ctx.channel();
            // 暂且不创建消费端的连接池
            Map<String, Set<Channel>> consumeClientRecord = topConsumeClientRecord.get(request.getTopic());
            if (consumeClientRecord == null) {
                consumeClientRecord = new ConcurrentHashMap<>();
            }
            Set<Channel> clients = consumeClientRecord.get(request.getConsumerGroup());
            if (clients == null) {
                clients = new ConcurrentSet<>();
            }
            clients.add(channel);
            consumeClientRecord.put(request.getConsumerGroup(), clients);
            topConsumeClientRecord.put(request.getTopic(), consumeClientRecord);

            // 响应消费
            MQMessage msg = new MQMessage(MQMsgType.MQSubscribeAck, MQMsgSource.MQCluster, new SimpleMessage());
            ctx.writeAndFlush(msg);
        }

        // Broker消息存储确认
        // Note:
        //  Producer消息确认
        private void doMsgStoreAck(MQMessage mqMessage) {
            AckMessageImpl ackMessage = (AckMessageImpl) mqMessage.getMsg();
            String address = ackMessage.getProducerAddress();
            Channel channel = producerRoute.get(address);
            channel.writeAndFlush(mqMessage);
        }

        // Producer消息存储
        // Note:
        //  1: 路由Broker
        //  2: 消息存储成功
        private void doMsgStore(ChannelHandlerContext ctx, MQMessage mqMessage) throws Exception {
            Channel channel = ctx.channel();
            String producerAddress = Utils.socketAddressCastString((InetSocketAddress) channel.remoteAddress());
            producerRoute.put(producerAddress, channel);

            TSMessage tsMessage = (TSMessage) mqMessage.getMsg();
            //
            String topicPath = ZK_TOPIC_PATH + "/" + tsMessage.getTopic();
            List<String> brokerServerList = zkClientContext.getChildNode(topicPath);
            BrokerNode brokerNode;
            if (brokerServerList == null || brokerServerList.isEmpty()) {
                brokerNode = loadBalance();
            } else {
                // 暂且只存储在一个Broker服务
                String brokerId = brokerServerList.get(0);
                byte []dataByte = zkClientContext.getNodeData(topicPath + "/" + brokerId);
                String data = new String(dataByte);
                if (Strings.isNotEmpty(data)) {
                    BrokerMessageHandler.TopicNodeData topicNodeData = GsonUtils.fromJson(data, BrokerMessageHandler.TopicNodeData.class);
                    brokerNode = new BrokerNode(topicNodeData.getBrokerId(), Utils.stringCastSocketAddress(topicNodeData.getBrokerServer(), ":"));
                } else {
                    brokerNode = loadBalance();
                }
            }

            // 消息存储
            BrokerTransportPool pool = connectors.get(brokerNode);
            BrokerTransport transport = pool.borrowObject();
            transport.writeAndFlush(mqMessage);
            pool.returnObject(transport);
        }

        private void doMsgResponse() {

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
            BrokerNode node = new BrokerNode(UUID, socketAddress);
            BrokerTransportPool pool = connectors.get(node);
            if (pool == null) {
                pool = new BrokerTransportPool(brokerAddress, zkConfig.getMqConfig(), new MsgProcessHandler());
                connectors.put(node, pool);
            } else {
                String address = pool.getBrokerAddress();
                if (!address.equals(brokerAddress)) {
                    // Broker Server服务地址发生变化, 需重新创建连接
                    pool.destroy();
                    pool = new BrokerTransportPool(brokerAddress, zkConfig.getMqConfig(), new MsgProcessHandler());
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
            InetSocketAddress socketAddress = Utils.stringCastSocketAddress(brokerAddress, ":");
            BrokerNode node = new BrokerNode(UUID, socketAddress);
            connectors.remove(node);
        }
    }
}
