package com.sdu.activemq.network.server;

import com.sdu.activemq.network.utils.NettyUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Netty Server
 *
 * @author hanhan.zhang
 * */
public class NettyServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyServer.class);

    private NettyServerConfig config;

    private EventLoopGroup bossGroup;

    private EventLoopGroup workerGroup;

    private ChannelFuture channelFuture;

    private AtomicBoolean start = new AtomicBoolean(false);

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
                 .childHandler(config.getChannelHandler());

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

        // start
        channelFuture = bootstrap.bind(new InetSocketAddress(config.getHost(), config.getPort()));
        channelFuture.addListeners(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.channel().isActive()) {
                    start.set(true);
                    socketAddress = (InetSocketAddress) future.channel().localAddress();
                }
            }
        });
        channelFuture.syncUninterruptibly();

        if (start.get()) {
            LOGGER.info("server[{}] start success .", socketAddress);
        }
    }

    public InetSocketAddress getSocketAddress() {
        return socketAddress;
    }

    public boolean isServing() {
        return start.get();
    }

    public void stop(int awaitTime, TimeUnit timeUnit) {
        if (start.get()) {
            start.set(false);
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
