package com.sdu.activemq.utils;

import static com.sdu.activemq.utils.Const.ZK_BROKER_PATH;
import static com.sdu.activemq.utils.Const.ZK_TOPIC_PATH;

/**
 * @author hanhan.zhang
 * */
public class ZkUtils {

    // Broker Server启动注册ZK节点[/activeMQ/broker/brokerId]
    public static String brokerServerNode(String brokerAddress) {
        return ZK_BROKER_PATH + "/" + brokerAddress;
    }

    // Broker Server接收到消息注册节点[/activeMQ/topic/topicName/brokerId]
    public static String brokerTopicNode(String topic, String brokerAddress) {
        return ZK_TOPIC_PATH + "/" + topic + "/" + brokerAddress;
    }

    public static String brokerTopicNode(String topic) {
        return ZK_TOPIC_PATH + "/" + topic;
    }

}
