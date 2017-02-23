package com.sdu.activemq.network.client;

import com.sdu.activemq.network.utils.NettyUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author hanhan.zhang
 * */
public class NettyClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyClient.class);

    private EventLoopGroup eventLoopGroup;

    private Channel channel;

    private NettyClientConfig config;

    private InetSocketAddress localSocketAddress;

    private InetSocketAddress remoteSocketAddress;

    public NettyClient(NettyClientConfig config) {
        this.config = config;
    }

    public void start() {
        eventLoopGroup = NettyUtils.createEventLoopGroup(config.isEPool(), config.getSocketThreads(), config.getClientThreadFactory());

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.channel(NettyUtils.getClientChannelClass(config.isEPool()))
                 .handler(config.getChannelHandler());

        if (config.getOptions() != null) {
            for (Map.Entry<ChannelOption, Object> entry : config.getOptions().entrySet()) {
                bootstrap.option(entry.getKey(), entry.getValue());
            }
        }

        ChannelFuture channelFuture = bootstrap.connect(NettyUtils.getInetSocketAddress(config.getRemoteAddress()));

        channelFuture.addListeners(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                channel = future.channel();
                localSocketAddress = (InetSocketAddress) channel.localAddress();
                remoteSocketAddress = (InetSocketAddress) channel.remoteAddress();
                LOGGER.info("connect remote address : {}", remoteSocketAddress);
            }
        });
    }

    public InetSocketAddress getLocalSocketAddress() {
        return localSocketAddress;
    }

    public InetSocketAddress getRemoteSocketAddress() {
        return remoteSocketAddress;
    }

    public ChannelFuture writeAndFlush(Object object) {
        return channel.writeAndFlush(object);
    }

    public void stop(int await, TimeUnit unit) throws InterruptedException {
        if (channel != null) {
            channel.closeFuture().sync();
        }
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully(await, await, unit);
        }
    }
}
