package com.sdu.activemq.core.broker;

import com.google.common.collect.Maps;
import com.sdu.activemq.core.MQConfig;
import com.sdu.activemq.core.zk.ZkClientContext;
import com.sdu.activemq.core.zk.ZkConfig;
import com.sdu.activemq.model.MQMessage;
import com.sdu.activemq.network.serialize.MessageObjectDecoder;
import com.sdu.activemq.network.serialize.MessageObjectEncoder;
import com.sdu.activemq.network.serialize.kryo.KryoSerializer;
import com.sdu.activemq.network.server.NettyServer;
import com.sdu.activemq.network.server.NettyServerConfig;
import com.sdu.activemq.utils.Utils;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.*;

import static com.sdu.activemq.utils.Const.ZK_BROKER_PATH;

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

    private String UUID;

    private NettyServer nettyServer;

    private BrokerConfig brokerConfig;

    private ZkConfig zkConfig;

    private ZkClientContext zkClientContext;

    private KryoSerializer serialize;

    private ExecutorService es;


    public BrokerServer(MQConfig mqConfig) {
        this.brokerConfig = new BrokerConfig(mqConfig);
        this.zkConfig = new ZkConfig(mqConfig);
    }

    @Override
    public void start() throws Exception {
        MessageObjectDecoder decoder = new MessageObjectDecoder(serialize);
        MessageObjectEncoder encoder = new MessageObjectEncoder(serialize);
        doStartServer(decoder, encoder);
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
        es = new ThreadPoolExecutor(poolSize, poolSize, 5, TimeUnit.MINUTES, queue);

        //
        BrokerHandler brokerHandler = new BrokerHandler(es, UUID);

        // 序列化
        serialize = new KryoSerializer(MQMessage.class);

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
                ch.pipeline().addLast(brokerHandler);
            }
        });

        nettyServer = new NettyServer(nettyServerConfig);
        nettyServer.start();

        if (!nettyServer.isServing()) {
            throw new IllegalStateException("broker server start failed.");
        }

        // 连接zk
        zkClientContext = new ZkClientContext(zkConfig);
        zkClientContext.start();
        // 注册节点
        if (zkClientContext.isServing()) {
            //
            brokerHandler.setZkClientContext(zkClientContext);
            brokerHandler.setBrokerSocketAddress(nettyServer.getSocketAddress());

            InetSocketAddress socketAddress = nettyServer.getSocketAddress();
            String host = socketAddress.getHostName();
            int port = socketAddress.getPort();
            String broker = host + ":" + port;
            UUID = Utils.generateUUID();
            String path = zkClientContext.createNode(zkNodePath(UUID), broker.getBytes());
            LOGGER.info("broker server create zk node : {}", path);
        }
    }

    private String zkNodePath(String uuid) {
        return ZK_BROKER_PATH + "/" + uuid;
    }

    @Override
    public void shutdown() throws Exception {
        // 删除节点
        zkClientContext.deleteNode(zkNodePath(UUID));

        if (nettyServer != null) {
            nettyServer.stop(10, TimeUnit.SECONDS);
        }

        if (es != null) {
            es.shutdown();
        }

        if (zkClientContext != null) {
            zkClientContext.destroy();
        }
    }
}
