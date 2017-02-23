package com.sdu.activemq.core.broker;

import com.sdu.activemq.core.MQConfig;
import com.sdu.activemq.utils.Utils;

import java.io.IOException;

/**
 * Broker Server配置
 *
 * @author hanhan.zhang
 * */
public class BrokerConfig {

    // Broker Server服务地址
    private static final String BROKER_SERVER_HOST = "broker.server.host";

    // Broker Server绑定端口
    private static final String BROKER_SERVER_PORT = "broker.server.port";

    // Broker Server Socket接收缓冲区
    private static final String BROKER_SERVER_SOCKET_RCV_BUFFER = "broker.server.socket.rcv.buf";

    // Broker Server Socket发送缓冲区
    private static final String BROKER_SERVER_SOCKET_SND_BUFFER = "broker.server.socket.snd.buf";

    // Broker Server Socket IO线程数[Netty WorkerBossGroup线程数]
    private static final String BROKER_SERVER_SOCKET_IO_THREADS = "broker.server.socket.io.threads";

    // Broker Server业务线程数
    private static final String BROKER_SERVER_MQ_WORKER_THREAD = "broker.server.mq.worker.threads";

    // Broker Server业务线程任务队列
    private static final String BROKER_SERVER_MQ_QUEUE_SIZE = "broker.server.mq.queue.size";

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

    public int getBrokerMQQueueSize() {
        assert mqConfig != null;
        return mqConfig.getInt(BROKER_SERVER_MQ_QUEUE_SIZE, 0);
    }

    public int getBrokerWorkerThreads() {
        assert mqConfig != null;
        return mqConfig.getInt(BROKER_SERVER_MQ_WORKER_THREAD, Runtime.getRuntime().availableProcessors() * 2);
    }

}
