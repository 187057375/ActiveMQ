package com.sdu.activemq.core.handler;

import com.sdu.activemq.model.MQMessage;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author hanhan.zhang
 * */
public interface MessageHandler {

    public void handleMessage(ChannelHandlerContext ctx, MQMessage msg);

}
