package com.sdu.activemq.network.server;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.concurrent.ThreadFactory;

/**
 * Netty Server配置
 *
 * @author hanhan.zhang
 * */
@Setter
@Getter
public class NettyServerConfig {

    public ThreadFactory bossThreadFactory;

    public ThreadFactory workerThreadFactory;

    public Map<ChannelOption, Object> options;

    public Map<ChannelOption, Object> childOptions;

    public String host;

    public int port;

    public int socketThreads;

    public ChannelHandler channelHandler;

    public ChannelHandler childChannelHandler;

    public boolean ePoll;

}
