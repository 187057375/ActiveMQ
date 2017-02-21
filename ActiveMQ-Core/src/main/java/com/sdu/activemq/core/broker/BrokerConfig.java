package com.sdu.activemq.core.broker;

import com.sdu.activemq.core.MQConfig;
import com.sdu.activemq.utils.Utils;

import java.io.IOException;

/**
 *
 * */
public class BrokerConfig {

    public static final String BROKER_CLUSTER_HOST = "broker.cluster.host";

    public static final String BROKER_CLUSTER_PORT = "broker.cluster.port";

    public static final String BROKER_SERVER_HOST = "broker.server.host";

    public static final String BROKER_SERVER_PORT = "broker.server.port";

    public static final String BROKER_SERVER_SOCKET_RCV_BUFFER = "broker.server.socket.rcv.buf";

    public static final String BROKER_SERVER_SOCKET_SND_BUFFER = "broker.server.socket.snd.buf";

    public static final String BROKER_SERVER_SOCKET_IO_THREADS = "broker.server.socket.io.threads";

    public static final String BROKER_SERVER_MQ_WORKER_THREAD = "broker.server.mq.worker.threads";

    public static final String BROKER_SERVER_MQ_QUEUE_SIZE = "broker.server.mq.queue.size";

    private MQConfig mqConfig;

    public BrokerConfig(String config) throws IOException {
        mqConfig = new MQConfig(config);
    }

    public BrokerConfig(MQConfig mqConfig) {
        this.mqConfig = mqConfig;
    }

    public String getBrokerHost() {
        assert mqConfig != null;
        return mqConfig.getString(BROKER_SERVER_HOST, Utils.getIpV4());
    }

    public int getBrokerPort() {
        assert mqConfig != null;
        return mqConfig.getInt(BROKER_SERVER_PORT, 0);
    }

    public int getSocketThreads() {
        assert mqConfig != null;
        return mqConfig.getInt(BROKER_SERVER_SOCKET_IO_THREADS, Runtime.getRuntime().availableProcessors());
    }

    public int getSocketRcvBuf() {
        assert mqConfig != null;
        return mqConfig.getInt(BROKER_SERVER_SOCKET_RCV_BUFFER, 1024);
    }

    public int getSocketSndBuf() {
        assert mqConfig != null;
        return mqConfig.getInt(BROKER_SERVER_SOCKET_SND_BUFFER, 1024);
    }

    public int getBrokerMQQueusSize() {
        assert mqConfig != null;
        return mqConfig.getInt(BROKER_SERVER_MQ_QUEUE_SIZE, 0);
    }

    public int getBrokerWorkerThreads() {
        assert mqConfig != null;
        return mqConfig.getInt(BROKER_SERVER_MQ_WORKER_THREAD, Runtime.getRuntime().availableProcessors() * 2);
    }

    public String getClusterHost() {
        assert mqConfig != null;
        return mqConfig.getString(BROKER_CLUSTER_HOST, "127.0.0.1");
    }

    public int getClusterPort() {
        assert mqConfig != null;
        return mqConfig.getInt(BROKER_CLUSTER_PORT, 0);
    }

}
