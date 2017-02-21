package com.sdu.activemq.core.broker;

import com.google.common.collect.Maps;
import com.sdu.activemq.model.MQMessage;
import com.sdu.activemq.network.client.NettyClient;
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

import java.util.Map;
import java.util.concurrent.*;

/**
 *
 * @author hanhan.zhang
 * */
public class BrokerServer implements Server {

    private NettyClient nettyClient;

    private NettyServer nettyServer;

    private BrokerConfig config;

    private KryoSerializer serialize;

    private ExecutorService es;

    public BrokerServer(BrokerConfig config) {
        this.config = config;
    }

    @Override
    public void start() {
        MessageObjectDecoder decoder = new MessageObjectDecoder(serialize);
        MessageObjectEncoder encoder = new MessageObjectEncoder(serialize);
        doStartServer(decoder, encoder);

    }

    private void doStartCluster(MessageObjectDecoder decoder, MessageObjectEncoder encoder) {

    }

    private void doStartServer(MessageObjectDecoder decoder, MessageObjectEncoder encoder) {
        // 工作线程
        int poolSize = config.getBrokerWorkerThreads();
        int queueSize = config.getBrokerMQQueusSize();
        BlockingQueue<Runnable> queue;
        if (queueSize == 0) {
            queue = new LinkedBlockingQueue<>();
        } else {
            queue = new ArrayBlockingQueue<>(queueSize);
        }
        es = new ThreadPoolExecutor(poolSize, poolSize, 5, TimeUnit.MINUTES, queue);

        //
        BrokerHandler handler = new BrokerHandler(es);

        // 序列化
        serialize = new KryoSerializer(MQMessage.class);

        // Netty Serve配置
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setBossThreadFactory(Utils.buildThreadFactory("broker-accept-thread-%d"));
        nettyServerConfig.setWorkerThreadFactory(Utils.buildThreadFactory("broker-socket-thread-%d"));
        nettyServerConfig.setEPoll(false);
        nettyServerConfig.setSocketThreads(config.getSocketThreads());
        nettyServerConfig.setHost(config.getBrokerHost());
        nettyServerConfig.setPort(config.getBrokerPort());
        nettyServerConfig.setChannelHandler(new LoggingHandler(LogLevel.INFO));

        // Netty Server Socket配置
        Map<ChannelOption, Object> options = Maps.newHashMap();
        options.put(ChannelOption.SO_BACKLOG, 1024);
        options.put(ChannelOption.SO_REUSEADDR, true);
        options.put(ChannelOption.SO_KEEPALIVE, false);
        options.put(ChannelOption.SO_SNDBUF, config.getSocketSndBuf());
        options.put(ChannelOption.SO_RCVBUF, config.getSocketRcvBuf());
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
                ch.pipeline().addLast(handler);
            }
        });

        nettyServer = new NettyServer(nettyServerConfig);
        nettyServer.start();
    }

    @Override
    public void shutdown() {
        if (nettyServer != null) {
            nettyServer.stop(10, TimeUnit.SECONDS);
        }

        if (es != null) {
            es.shutdown();
        }
    }
}
