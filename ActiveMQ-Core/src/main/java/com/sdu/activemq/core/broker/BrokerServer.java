package com.sdu.activemq.core.broker;

import com.google.common.collect.Maps;
import com.sdu.activemq.core.MQConfig;
import com.sdu.activemq.core.zk.ZkClientContext;
import com.sdu.activemq.core.zk.ZkConfig;
import com.sdu.activemq.msg.MQMessage;
import com.sdu.activemq.network.serialize.MessageObjectDecoder;
import com.sdu.activemq.network.serialize.MessageObjectEncoder;
import com.sdu.activemq.network.serialize.kryo.KryoSerializer;
import com.sdu.activemq.network.server.NettyServer;
import com.sdu.activemq.network.server.NettyServerConfig;
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

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Broker Server职责
 *
 *  1: MQ消息存储
 *
 *  2: Zk节点注册/更改
 *
 *      1': Broker Server启动注册节点[/activeMQ/broker/brokerId]
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
        KryoSerializer kryoSerializer = new KryoSerializer(MQMessage.class);
        MessageObjectDecoder decoder = new MessageObjectDecoder(kryoSerializer);
        MessageObjectEncoder encoder = new MessageObjectEncoder(kryoSerializer);
        doStartServer(decoder, encoder);
        // JVM退出钩子
        Runtime.getRuntime().addShutdownHook(new ShutdownHook());
    }

    private void doStartServer(MessageObjectDecoder decoder, MessageObjectEncoder encoder) throws Exception {
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

        //
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
                ch.pipeline().addLast(decoder);
                ch.pipeline().addLast(encoder);
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
            InetSocketAddress socketAddress = nettyServer.getSocketAddress();
            String host = socketAddress.getHostName();
            int port = socketAddress.getPort();
            String broker = host + ":" + port;
            brokerId = Utils.generateUUID();
            String path = zkClientContext.createNode(ZkUtils.brokerServerNode(brokerId), broker.getBytes());
            LOGGER.info("broker server create zk node : {}", path);
        }
    }

    @Override
    public void shutdown() throws Exception {
        // 删除节点
        zkClientContext.deleteNode(ZkUtils.brokerServerNode(brokerId));

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
