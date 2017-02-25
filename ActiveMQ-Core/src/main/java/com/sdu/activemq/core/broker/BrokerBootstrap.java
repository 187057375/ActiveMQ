package com.sdu.activemq.core.broker;

import com.sdu.activemq.core.MQConfig;

/**
 * @author hanhan.zhang
 * */
public class BrokerBootstrap {

    public static void main(String[] args) throws Exception {
        MQConfig mqConfig = new MQConfig("broker.cfg");
        BrokerServer brokerServer = new BrokerServer(mqConfig);
        brokerServer.start();
    }

}
