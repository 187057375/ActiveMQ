package com.sdu.activemq.core.cluster;

import com.sdu.activemq.core.MQConfig;

/**
 * @author hanhan.zhang
 * */
public class ClusterBootStrap {

    public static void main(String[] args) throws Exception {
        MQConfig mqConfig = new MQConfig("cluster.cfg");
        BrokerCluster cluster = new BrokerCluster(mqConfig);
        cluster.start();
    }

}
