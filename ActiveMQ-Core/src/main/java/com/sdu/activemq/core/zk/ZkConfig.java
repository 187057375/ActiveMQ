package com.sdu.activemq.core.zk;

import com.sdu.activemq.core.MQConfig;

/**
 *
 * @author hanhan.zhang
 * */
public class ZkConfig {

    private static final String ZK_SERVER_ADDRESS = "zk.server.address";

    private static final String ZK_SERVER_CONNECT_TIMEOUT = "zk.server.connect.timeout";

    private static final String ZK_SERVER_RETRY_TIMES = "zk.server.retry.times";

    private static final String ZK_SERVER_RETRY_SLEEP_INTERVAL = "zk.server.retry.sleep.interval";

    private static final String ZK_SERVER_PATH = "zk.server.path";

    private MQConfig mqConfig;

    public ZkConfig(MQConfig mqConfig) {
        this.mqConfig = mqConfig;
    }

    public String getZkServer() {
        return mqConfig.getString(ZK_SERVER_ADDRESS, "127.0.0.1:2181");
    }

    public int getZkServerConnectTimeout() {
        return mqConfig.getInt(ZK_SERVER_CONNECT_TIMEOUT, 1000);
    }

    public int getZkRetryTimes() {
        return mqConfig.getInt(ZK_SERVER_RETRY_TIMES, 3);
    }

    public int getZkRetrySleepInterval() {
        return mqConfig.getInt(ZK_SERVER_RETRY_SLEEP_INTERVAL, 1000);
    }

    public String getZkPath() {
        return mqConfig.getString(ZK_SERVER_PATH, "/");
    }

    public MQConfig getMqConfig() {
        return mqConfig;
    }
}
