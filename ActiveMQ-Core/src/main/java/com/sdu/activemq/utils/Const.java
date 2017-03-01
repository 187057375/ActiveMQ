package com.sdu.activemq.utils;

/**
 * @author hanhan.zhang
 * */
public class Const {

    // Broker Server注册节点路径
    public static final String ZK_BROKER_PATH = "/activeMQ/broker";

    // 主题消息注册节点路径
    public static final String ZK_TOPIC_META_PATH = "/activeMQ/topic";

    // 主题消息存储节点路径
    public static final String ZK_MSG_DATA_PATH = "/activeMQ/message";

    // 主题消息订阅节点路径
    public static final String ZK_MSG_CONSMUE_PATH = "/activeMQ/consume";

    public static final String ZK_MQ_LOCK_PATH = "/activeMQ/lock";

}
