package com.sdu.activemq.core.cluster;

import com.sdu.activemq.core.MQConfig;
import com.sdu.activemq.core.broker.client.BrokerTransportPool;
import com.sdu.activemq.core.broker.client.BrokerTransport;
import com.sdu.activemq.core.zk.ZkClientContext;
import com.sdu.activemq.core.zk.ZkConfig;
import com.sdu.activemq.model.MQMessage;
import com.sdu.activemq.utils.Utils;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

import static com.sdu.activemq.utils.Const.ZK_BROKER_PATH;

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

    // Zk
    private ZkClientContext zkClientContext;

    private ZkConfig zkConfig;

    public BrokerCluster(MQConfig mqConfig) {
        zkConfig = new ZkConfig(mqConfig);
        connectors = new ConcurrentHashMap<>();

    }

    @Override
    public void start() throws Exception {
        zkClientContext = new ZkClientContext(zkConfig);
        zkClientContext.start();
        zkClientContext.addPathListener(ZK_BROKER_PATH, true, new BrokerPathChildrenCacheListener());
    }

    @Override
    public BrokerTransport getConnector(MQMessage msg) {
        return null;
    }

    @Override
    public void destroy() throws Exception {
        zkClientContext.destroy();
    }

    private class MsgProcessHandler extends ChannelInboundHandlerAdapter {



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
