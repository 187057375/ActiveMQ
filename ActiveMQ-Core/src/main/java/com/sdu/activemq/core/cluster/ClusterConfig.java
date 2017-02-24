package com.sdu.activemq.core.cluster;

import com.sdu.activemq.core.MQConfig;

/**
 * @author hanhan.zhang
 * */
public class ClusterConfig {

    private static final String CLUSTER_SOCKET_THREAD = "cluster.socket.thread";

    private static final String CLUSTER_ADDRESS_HOST = "cluster.address.host";

    private static final String CLUSTER_ADDRESS_PORT = "cluster.address.port";

    private static final String CLUSTER_SOCKET_RCV_BUFFER = "cluster.socket.rcv.buffer";

    private static final String CLUSTER_SOCKET_SND_BUFFER = "cluster.socket.snd.buffer";

    private MQConfig config;

    public ClusterConfig(MQConfig config) {
        this.config = config;
    }

    public int getClusterSocketThread() {
        return config.getInt(CLUSTER_SOCKET_THREAD, 50);
    }

    public String getClusterAddressHost() {
        return config.getString(CLUSTER_ADDRESS_HOST, "127.0.0.1");
    }

    public int getClusterAddressPort() {
        return config.getInt(CLUSTER_ADDRESS_PORT, 6712);
    }

    public int getClusterSocketRcvBuf() {
        return config.getInt(CLUSTER_SOCKET_RCV_BUFFER, 1024);
    }

    public int getClusterSocketSndBuf() {
        return config.getInt(CLUSTER_SOCKET_SND_BUFFER, 1024);
    }
}
