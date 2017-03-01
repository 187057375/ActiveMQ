package com.sdu.activemq.network.client;

import com.sdu.activemq.network.utils.NettyUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author hanhan.zhang
 * */
public class NettyClient {

    // Netty Socket Thread管理组
    private EventLoopGroup eventLoopGroup;

    // Netty NioSocketChannel
    private Channel channel;

    private NettyClientConfig config;

    private AtomicBoolean started = new AtomicBoolean(false);

    public NettyClient(NettyClientConfig config) {
        this.config = config;
    }

    public void start() {
        eventLoopGroup = NettyUtils.createEventLoopGroup(config.isEPool(), config.getSocketThreads(), config.getClientThreadFactory());

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup)
                 .channel(NettyUtils.getClientChannelClass(config.isEPool()))
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
                started.set(true);
                channel = future.channel();
            }
        });
    }

    public ChannelFuture writeAndFlush(Object object) {
        if (!isStarted()) {
            throw new IllegalStateException("client not started !");
        }
        return channel.writeAndFlush(object);
    }

    public boolean isStarted() {
        return started.get();
    }

    public void stop(int await, TimeUnit unit) throws InterruptedException {
        started.set(false);
        if (channel != null) {
            channel.closeFuture().sync();
        }
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully(await, await, unit);
        }
    }
}
