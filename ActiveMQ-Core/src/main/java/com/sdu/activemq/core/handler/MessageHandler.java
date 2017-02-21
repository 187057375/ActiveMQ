package com.sdu.activemq.core.handler;

import io.netty.channel.ChannelHandlerContext;

/**
 * @author hanhan.zhang
 * */
public interface MessageHandler {

    public void handleMessage(ChannelHandlerContext ctx, Object msg);

}
