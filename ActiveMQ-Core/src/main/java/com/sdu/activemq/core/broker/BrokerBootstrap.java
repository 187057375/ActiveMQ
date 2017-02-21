package com.sdu.activemq.core.broker;

import java.io.IOException;

/**
 * @author hanhan.zhang
 * */
public class BrokerBootstrap {

    public static void start(String config) throws IOException {
        // 配置
        BrokerConfig brokerConfig = new BrokerConfig(config);

        BrokerServer server = new BrokerServer(brokerConfig);

        server.start();
    }

    public static void main(String[] args) {

    }

}
