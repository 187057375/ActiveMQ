package com.sdu.activemq.core.route;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MQ消息分发存储
 *
 * @author hanhan.zhang
 * */
public class MessageDistributeHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageDistributeHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        super.channelRead(ctx, msg);
        // MQ消息分发耗时, 应交给后台线程处理, 而非在Netty Socket线程中运行

    }
}
