package com.sdu.activemq.core.cluster.broker;

import com.google.common.collect.Maps;
import com.sdu.activemq.core.MQConfig;
import com.sdu.activemq.core.zk.ZkClientContext;
import com.sdu.activemq.core.zk.ZkConfig;
import com.sdu.activemq.core.zk.node.ZkBrokerNode;
import com.sdu.activemq.msg.MQMessage;
import com.sdu.activemq.network.serialize.MessageObjectDecoder;
import com.sdu.activemq.network.serialize.MessageObjectEncoder;
import com.sdu.activemq.network.serialize.kryo.KryoSerializer;
import com.sdu.activemq.network.server.NettyServer;
import com.sdu.activemq.network.server.NettyServerConfig;
import com.sdu.activemq.utils.GsonUtils;
import com.sdu.activemq.utils.Utils;
import com.sdu.activemq.utils.ZkUtils;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

/**
 * Broker Server职责
 *
 *  1: MQ消息存储
 *
 *  2: Zk节点注册/更改
 *
 *      1': Broker Server启动并创建临时节点[/activeMQ/broker/brokerId]
 *
 *      2': Broker Server消息存储成功更改节点[/activeMQ/topic/topicName/brokerId]
 *
 * @author hanhan.zhang
 * */
public class BrokerServer implements Server {

    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerServer.class);

    @Getter
    private String brokerId;

    @Getter
    private NettyServer nettyServer;

    private BrokerConfig brokerConfig;

    private ZkConfig zkConfig;

    @Getter
    private ZkClientContext zkClientContext;

    @Getter
    private ExecutorService executorService;

    public BrokerServer(MQConfig mqConfig) {
        this.brokerConfig = new BrokerConfig(mqConfig);
        this.zkConfig = new ZkConfig(mqConfig);
    }

    @Override
    public void start() throws Exception {
        doStartServer();
        // JVM退出钩子
        Runtime.getRuntime().addShutdownHook(new ShutdownHook());
    }

    private void doStartServer() throws Exception {
        // 工作线程
        int poolSize = brokerConfig.getBrokerWorkerThreads();
        int queueSize = brokerConfig.getBrokerMQQueueSize();
        BlockingQueue<Runnable> queue;
        if (queueSize == 0) {
            queue = new LinkedBlockingQueue<>();
        } else {
            queue = new ArrayBlockingQueue<>(queueSize);
        }
        executorService = new ThreadPoolExecutor(poolSize, poolSize, 5, TimeUnit.MINUTES, queue);

        BrokerMessageHandler brokerMessageHandler = new BrokerMessageHandler(this);

        // Netty Serve配置
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setBossThreadFactory(Utils.buildThreadFactory("broker-accept-thread-%d"));
        nettyServerConfig.setWorkerThreadFactory(Utils.buildThreadFactory("broker-socket-thread-%d"));
        nettyServerConfig.setEPoll(false);
        nettyServerConfig.setSocketThreads(brokerConfig.getSocketThreads());
        nettyServerConfig.setHost(brokerConfig.getBrokerHost());
        nettyServerConfig.setPort(brokerConfig.getBrokerPort());
        nettyServerConfig.setChannelHandler(new LoggingHandler(LogLevel.INFO));

        // Netty Server Socket配置
        Map<ChannelOption, Object> options = Maps.newHashMap();
        options.put(ChannelOption.SO_BACKLOG, 1024);
        options.put(ChannelOption.SO_REUSEADDR, true);
        options.put(ChannelOption.SO_KEEPALIVE, false);
        options.put(ChannelOption.SO_SNDBUF, brokerConfig.getSocketSndBuf());
        options.put(ChannelOption.SO_RCVBUF, brokerConfig.getSocketRcvBuf());
        options.put(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        nettyServerConfig.setOptions(options);

        Map<ChannelOption, Object> childOptions = Maps.newHashMap();
        childOptions.put(ChannelOption.TCP_NODELAY, true);
        childOptions.put(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        nettyServerConfig.setChildOptions(childOptions);
        nettyServerConfig.setChildChannelHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                // 设置Socket数据通信编码
                // Note:
                //  1: Decode/Encode不可共享
                //  2: 重写isSharable()
                KryoSerializer kryoSerializer = new KryoSerializer(MQMessage.class);
                ch.pipeline().addLast(new MessageObjectDecoder(kryoSerializer));
                ch.pipeline().addLast(new MessageObjectEncoder(kryoSerializer));
                ch.pipeline().addLast(brokerMessageHandler);
            }
        });

        nettyServer = new NettyServer(nettyServerConfig);
        nettyServer.start();

        nettyServer.blockUntilStarted(2);

        if (!nettyServer.isServing()) {
            throw new IllegalStateException("broker server start failed.");
        }

        LOGGER.info("broker server start success, address = {}", Utils.socketAddressCastString(nettyServer.getSocketAddress()));

        // 连接zk
        zkClientContext = new ZkClientContext(zkConfig);
        zkClientContext.start();
        // 注册节点
        if (zkClientContext.isServing()) {
            brokerId = Utils.generateUUID();
            String brokerAddress = getServerAddress();
            ZkBrokerNode zkNode = new ZkBrokerNode(brokerAddress, brokerId);
            String path = zkClientContext.createNode(ZkUtils.zkBrokerNode(brokerAddress), GsonUtils.toJson(zkNode));
            LOGGER.info("broker server create zk node : {}", path);
        }
    }

    @Override
    public void shutdown() throws Exception {
        // 删除节点
        zkClientContext.deleteNode(ZkUtils.zkBrokerNode(brokerId));

        if (nettyServer != null) {
            nettyServer.stop(10, TimeUnit.SECONDS);
        }

        if (executorService != null) {
            executorService.shutdown();
        }

        if (zkClientContext != null) {
            zkClientContext.destroy();
        }
    }

    @Override
    public String getServerAddress() {
        return Utils.socketAddressCastString(nettyServer.getSocketAddress());
    }

    private class ShutdownHook extends Thread {
        @Override
        public void run() {
            try {
                shutdown();
            } catch (Exception e) {
                //
            }
        }
    }
}