package com.sdu.activemq.utils;

import static com.sdu.activemq.utils.Const.ZK_BROKER_PATH;
import static com.sdu.activemq.utils.Const.ZK_TOPIC_META_PATH;
import static com.sdu.activemq.utils.Const.ZK_MSG_DATA_PATH;

/**
 * @author hanhan.zhang
 * */
public class ZkUtils {

    // Broker Server启动注册ZK节点[/activeMQ/broker/brokerId]
    public static String zkBrokerNode(String brokerAddress) {
        return ZK_BROKER_PATH + "/" + brokerAddress;
    }

    public static String zkTopicMetaNode(String topic) {
        return ZK_TOPIC_META_PATH + "/" + topic;
    }

    // Broker Server接收到消息注册节点[/activeMQ/topic/topicName/brokerId]
    public static String zkMsgDataNode(String topic, String brokerAddress) {
        return ZK_MSG_DATA_PATH + "/" + topic + "/" + brokerAddress;
    }

    public static String brokerTopicNode(String topic) {
        return ZK_MSG_DATA_PATH + "/" + topic;
    }

}
