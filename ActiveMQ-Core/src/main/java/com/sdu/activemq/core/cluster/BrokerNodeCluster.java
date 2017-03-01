package com.sdu.activemq.core.cluster;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sdu.activemq.core.MQConfig;
import com.sdu.activemq.core.transport.TransportPool;
import com.sdu.activemq.core.zk.ZkClientContext;
import com.sdu.activemq.core.zk.ZkConfig;
import com.sdu.activemq.core.zk.node.ZkBrokerNode;
import com.sdu.activemq.core.zk.node.ZkMsgDataNode;
import com.sdu.activemq.core.zk.node.ZkMsgTopicNode;
import com.sdu.activemq.utils.GsonUtils;
import com.sdu.activemq.utils.Utils;
import com.sdu.activemq.utils.ZkUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.logging.log4j.util.Strings;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.sdu.activemq.utils.Const.ZK_BROKER_PATH;
import static com.sdu.activemq.utils.Const.ZK_MSG_DATA_PATH;
import static org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type.NODE_UPDATED;

/**
 * Broker Cluster职责:
 *
 *  1: 监控Broker Server上线/下线
 *
 *  2: 消息存储负载均衡
 *
 *  3: 监听消息存储节点
 *
 * @author hanhan.zhang
 * */
public class BrokerNodeCluster {

    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerCluster.class);

    /**
     * Broker节点连接池
     * */
    private Map<BrokerNode, TransportPool> brokerTransportPool;

    private List<DataChangeListener> dataChangeListeners;

    private static BrokerNodeCluster cluster;

    private ZkClientContext zkClientContext;

    private MQConfig mqConfig;

    private BrokerNodeCluster(MQConfig mqConfig) throws Exception {
        this.mqConfig = mqConfig;
        brokerTransportPool = Maps.newConcurrentMap();
        this.dataChangeListeners = Lists.newArrayList();
        connectZk(mqConfig);
    }

    public static synchronized BrokerNodeCluster getCluster(MQConfig mqConfig) {
        if (cluster == null) {
            try {
                cluster = new BrokerNodeCluster(mqConfig);
            } catch (Exception e) {
                throw new IllegalStateException("connect zk failure !");
            }
        }
        return cluster;
    }

    public BrokerNode getBrokerNode(String topic) throws Exception {
        String data = new String(zkClientContext.getNodeData(ZkUtils.zkTopicMetaNode(topic)));
        if (Strings.isNotEmpty(data)) {
            ZkMsgTopicNode storeNode = GsonUtils.fromJson(data, ZkMsgTopicNode.class);
            if (storeNode != null) {
                InetSocketAddress socketAddress = Utils.stringCastSocketAddress(storeNode.getBrokerAddress(), ":");
                return new BrokerNode(storeNode.getBrokerId(), socketAddress);
            }
        }

        // 随机选择一个节点
        List<BrokerNode> brokerNodes = Lists.newLinkedList(brokerTransportPool.keySet());
        Collections.shuffle(brokerNodes);
        BrokerNode brokerNode = brokerNodes.get(0);

        // 创建ZK Store节点
        String brokerAddress = Utils.socketAddressCastString(brokerNode.getSocketAddress());
        ZkMsgTopicNode storeNode = new ZkMsgTopicNode(brokerAddress, brokerNode.getBrokerID(), topic);
        zkClientContext.createNode(ZkUtils.zkTopicMetaNode(topic), GsonUtils.toJson(storeNode), CreateMode.PERSISTENT);

        return brokerNode;
    }

    public TransportPool getTransportPool(BrokerNode brokerNode) {
        return brokerTransportPool.get(brokerNode);
    }

    public synchronized void addDataChangeListener(DataChangeListener listener) {
        dataChangeListeners.add(listener);
    }

    public void destroy() throws Exception {
        if (zkClientContext != null && zkClientContext.isServing()) {
            zkClientContext.destroy();
        }

        if (brokerTransportPool != null) {
            for (Map.Entry<BrokerNode, TransportPool> entry : brokerTransportPool.entrySet()) {
                entry.getValue().destroy();
            }
        }

        cluster = null;
    }

    /**
     * 连接ZK并监听节点[/activeMQ/broker/brokerId]变化
     * */
    private void connectZk(MQConfig mqConfig) throws Exception {
        zkClientContext = new ZkClientContext(new ZkConfig(mqConfig));
        zkClientContext.start();

        // Broker Server上线/下线监控
        zkClientContext.addPathListener(ZK_BROKER_PATH, true, new BrokerPathChildrenCacheListener());
        // 监控消息节点变化
        zkClientContext.addSubAllPathListener(ZK_MSG_DATA_PATH, new TopicMessagePathChildrenCacheListener());
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

            InetSocketAddress socketAddress = Utils.stringCastSocketAddress(zkNodeData.getBrokerAddress(), ":");
            BrokerNode node = new BrokerNode(zkNodeData.getBrokerId(), socketAddress);
            TransportPool pool = brokerTransportPool.get(node);
            if (pool == null) {
                pool = new TransportPool(zkNodeData.getBrokerAddress(), mqConfig);
                brokerTransportPool.put(node, pool);
            } else {
                String address = pool.getBrokerAddress();
                if (!address.equals(zkNodeData.getBrokerAddress())) {
                    // Server服务地址发生变化, 需重新创建连接
                    pool.destroy();
                    pool = new TransportPool(zkNodeData.getBrokerAddress(), mqConfig);
                    brokerTransportPool.put(node, pool);
                }
            }
        }

        // Broker服务节点下线
        private void deleteBroker(ChildData childData) {
            if (childData == null || childData.getData() == null || childData.getData().length == 0) {
                return;
            }

            ZkBrokerNode zkNodeData = GsonUtils.fromJson(new String(childData.getData()), ZkBrokerNode.class);

            String brokerAddress = zkNodeData.getBrokerAddress();

            LOGGER.info("broker server node[{}] offline and update broker cluster .", brokerAddress);

            InetSocketAddress socketAddress = Utils.stringCastSocketAddress(brokerAddress, ":");
            BrokerNode node = new BrokerNode(zkNodeData.getBrokerId(), socketAddress);
            brokerTransportPool.remove(node);
        }
    }

    private class TopicMessagePathChildrenCacheListener implements TreeCacheListener {

        @Override
        public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
            TreeCacheEvent.Type type = event.getType();
            if (type == NODE_UPDATED) {
                topicChangedAndRequest(event.getData());
            }
        }

        // Topic消息发生变化, 向Broker发送消息请求
        // Note:
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
            }
        }
    }
}
