package com.sdu.activemq.network.client;

import com.sdu.activemq.network.utils.NettyUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author hanhan.zhang
 * */
public class NettyClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyClient.class);

    private EventLoopGroup eventLoopGroup;

    private ChannelFuture channelFuture;

    private NettyClientConfig config;

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

        channelFuture = bootstrap.connect(NettyUtils.getInetSocketAddress(config.getRemoteAddress()));

        channelFuture.addListeners(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                Channel channel = future.channel();
                LOGGER.info("connect remote address : {}", channel.remoteAddress());
            }
        });
    }

    public void stop(int await, TimeUnit unit) throws InterruptedException {
        if (channelFuture != null) {
            channelFuture.channel().closeFuture().sync();
        }
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully(await, await, unit);
        }
    }
}
