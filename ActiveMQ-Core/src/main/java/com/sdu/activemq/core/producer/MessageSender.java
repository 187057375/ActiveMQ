package com.sdu.activemq.core.producer;

import com.google.common.collect.Maps;
import com.sdu.activemq.core.cluster.BrokerNode;
import com.sdu.activemq.core.cluster.BrokerNodeCluster;
import com.sdu.activemq.core.disruptor.MessageHandler;
import com.sdu.activemq.core.transport.DataTransport;
import com.sdu.activemq.core.transport.TransportPool;
import com.sdu.activemq.msg.MQMessage;
import com.sdu.activemq.msg.MQMsgType;
import com.sdu.activemq.msg.MsgAckImpl;
import com.sdu.activemq.msg.MsgContent;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 *
 * @author hanhan.zhang
 * */
public class MessageSender implements MessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageSender.class);

    private static final String CHANNEL_HANDLER_NAME = "producer.channel.handler";

    private Map<String, MQMessage> sendPending;

    private BrokerNodeCluster cluster;

    private MessageChannelHandler channelHandler;

    public MessageSender(BrokerNodeCluster cluster) {
        this.cluster = cluster;
        this.channelHandler = new MessageChannelHandler();
        this.sendPending = Maps.newConcurrentMap();
    }

    @Override
    public void handle(Object msg) {
        if (msg.getClass() != MQMessage.class) {
            return;
        }
        MQMessage mqMessage = (MQMessage) msg;
        MsgContent content = (MsgContent) mqMessage.getMsg();

        try {
            BrokerNode brokerNode = cluster.getBrokerNode(content.getTopic());
            TransportPool pool = cluster.getTransportPool(brokerNode);
            DataTransport transport = pool.borrowObject(CHANNEL_HANDLER_NAME, channelHandler);
            transport.writeAndFlush(msg);
            pool.returnObject(transport);
        } catch (Exception e) {
            LOGGER.error("send msg exception", e);
        }
    }




    @ChannelHandler.Sharable
    private class MessageChannelHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg.getClass() == MQMessage.class) {
                MQMessage mqMessage = (MQMessage) msg;
                MQMsgType type = mqMessage.getMsgType();
                switch (type) {
                    case MQMsgStoreAck:
                        doMsgStoreAck(mqMessage);
                        break;
                }
            }
        }

        // 主题消息存储确认
        private void doMsgStoreAck(MQMessage mqMessage) {
            MsgAckImpl ackMessage = (MsgAckImpl) mqMessage.getMsg();
            switch (ackMessage.getAckStatus()) {
                case SUCCESS:
                    LOGGER.info("producer receive ack : topic = {}, msgId = {}", ackMessage.getTopic(), ackMessage.getMsgId());
                    sendPending.remove(ackMessage.getMsgId());
                    break;
                case FAILURE:
                    break;
            }
        }


    }


}
