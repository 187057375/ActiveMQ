package com.sdu.activemq.network.server;

import com.sdu.activemq.network.utils.NettyUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Netty Server
 *
 * @author hanhan.zhang
 * */
public class NettyServer {

    private NettyServerConfig config;

    private EventLoopGroup bossGroup;

    private EventLoopGroup workerGroup;

    private ChannelFuture channelFuture;

    private AtomicBoolean started = new AtomicBoolean(false);

    // 绑定的服务地址
    private InetSocketAddress socketAddress;

    public NettyServer(NettyServerConfig config) {
        this.config = config;
    }

    public void start() {
        bossGroup = NettyUtils.createEventLoopGroup(config.isEPoll(), 1, config.getBossThreadFactory());
        workerGroup = NettyUtils.createEventLoopGroup(config.isEPoll(), config.getSocketThreads(), config.getWorkerThreadFactory());

        //
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                 .channel(NettyUtils.getServerChannelClass(config.isEPoll()))
                 .childHandler(config.getChildChannelHandler());

        if (config.getOptions() != null) {
            for (Map.Entry<ChannelOption, Object> entry : config.getOptions().entrySet()) {
                bootstrap.option(entry.getKey(), entry.getValue());
            }
        }

        if (config.getChildOptions() != null) {
            for (Map.Entry<ChannelOption, Object> entry : config.getChildOptions().entrySet()) {
                bootstrap.childOption(entry.getKey(), entry.getValue());
            }
        }

        // started
        channelFuture = bootstrap.bind(new InetSocketAddress(config.getHost(), config.getPort()));
        channelFuture.addListeners(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.channel().isActive()) {
                    started.set(true);
                    socketAddress = (InetSocketAddress) future.channel().localAddress();
                }
            }
        });

        channelFuture.syncUninterruptibly();
    }

    public InetSocketAddress getSocketAddress() {
        return socketAddress;
    }

    public void blockUntilStarted(long seconds) throws InterruptedException {
        synchronized (this) {
            long maxWaitTimeMs = TimeUnit.MILLISECONDS.convert(seconds, TimeUnit.SECONDS);
            wait(maxWaitTimeMs);
        }
    }

    public boolean isServing() {
        return started.get();
    }

    public void stop(int awaitTime, TimeUnit timeUnit) {
        if (started.get()) {
            started.set(false);
        }
        if (channelFuture != null) {
            channelFuture.channel().closeFuture().awaitUninterruptibly(awaitTime, timeUnit);
            channelFuture = null;
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(awaitTime, awaitTime, timeUnit);
            bossGroup = null;
        }

        if (workerGroup != null) {
            workerGroup.shutdownGracefully(awaitTime, awaitTime, timeUnit);
            workerGroup = null;
        }
    }
}
