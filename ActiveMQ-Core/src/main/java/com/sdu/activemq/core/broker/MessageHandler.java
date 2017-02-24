package com.sdu.activemq.core.broker;

import com.sdu.activemq.msg.MQMessage;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author hanhan.zhang
 * */
public interface MessageHandler {

    // 消息存储
    public void storeMessage(ChannelHandlerContext ctx, MQMessage msg);

    // 消息消费
    public void consumeMessage(ChannelHandlerContext ctx, MQMessage msg);
}
