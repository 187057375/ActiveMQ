package com.sdu.activemq.network.client;

import com.sdu.activemq.network.utils.NettyUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author hanhan.zhang
 * */
public class NettyClient {

    private EventLoopGroup eventLoopGroup;

    private Channel channel;

    private NettyClientConfig config;

    private InetSocketAddress localSocketAddress;

    private InetSocketAddress remoteSocketAddress;

    private AtomicBoolean start = new AtomicBoolean(false);

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
                start.set(true);
                channel = future.channel();
                localSocketAddress = (InetSocketAddress) channel.localAddress();
                remoteSocketAddress = (InetSocketAddress) channel.remoteAddress();
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

    public boolean isStarted() {
        return start.get();
    }

    public void stop(int await, TimeUnit unit) throws InterruptedException {
        start.set(false);
        if (channel != null) {
            channel.closeFuture().sync();
        }
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully(await, await, unit);
        }
    }
}
