package com.sdu.activemq.network.client;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.Map;
import java.util.concurrent.ThreadFactory;

/**
 * @author hanhan.zhang
 * */
@Setter
@Getter
public class NettyClientConfig {

    private boolean ePool;

    private int socketThreads;

    private ThreadFactory clientThreadFactory;

    private Map<ChannelOption, Object> options;

    private ChannelHandler channelHandler;

    @NonNull
    private String remoteAddress;

}
