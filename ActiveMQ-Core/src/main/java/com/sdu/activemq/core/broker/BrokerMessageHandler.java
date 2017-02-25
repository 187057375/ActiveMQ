package com.sdu.activemq.core.broker;

import com.google.common.base.Strings;
import com.sdu.activemq.core.store.MemoryMsgStore;
import com.sdu.activemq.msg.*;
import com.sdu.activemq.utils.GsonUtils;
import com.sdu.activemq.utils.Utils;
import com.sdu.activemq.utils.ZkUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.sdu.activemq.msg.MQMsgSource.MQBroker;
import static com.sdu.activemq.msg.MQMsgType.MQHeartBeatAck;

/**
 * MQ消息处理:
 *
 *  1: MQ消息存储
 *
 *  2: MQ消息消费
 *
 *  3: Cluster发送的心跳
 *
 * @author hanhan.zhang
 * */
public class BrokerMessageHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerMessageHandler.class);

    private BrokerServer brokerServer;

    private MessageHandler handler;

    private AtomicLong sequence = new AtomicLong(0L);

    private AtomicBoolean created = new AtomicBoolean(false);

    // 基于内存存储
    private MemoryMsgStore msgStore = new MemoryMsgStore();


    public BrokerMessageHandler(BrokerServer server) {
        this.brokerServer = server;

        // 生成代理
        handler = new MessageHandlerImpl();
        MessageInterceptor interceptor = new MessageInterceptorImpl();
        MessageInvoker invoker = new MessageInvoker(handler, interceptor, "storeMessage");
        Class<?>[] interceptorClazz = new Class[]{MessageHandler.class};
        handler = (MessageHandler) Proxy.newProxyInstance(MessageHandlerImpl.class.getClassLoader(), interceptorClazz, invoker);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg.getClass() != MQMessage.class) {
            return;
        }
        MQMessage mqMessage = (MQMessage) msg;
        MQMsgType type = mqMessage.getMsgType();
        switch (type) {
            case MQHeartBeat:
                doHeartbeat(ctx, mqMessage);
                break;
            case MQMessageStore:
                doMsgStore(ctx, mqMessage);
                break;
            case MQMessageRequest:
                doMsgConsume(ctx, mqMessage);
                break;
        }
    }

    // MQ消息存储
    private void doMsgStore(ChannelHandlerContext ctx, MQMessage msg) {
        try {
            brokerServer.getExecutorService().submit(() -> handler.storeMessage(ctx, msg));
        } catch (RejectedExecutionException e) {
            LOGGER.error("worker pool reject the task, msgId : {} .", msg.getMsgId(), e);
        }
    }

    private void doMsgConsume(ChannelHandlerContext ctx, MQMessage msg) {
        try {
            brokerServer.getExecutorService().submit(() -> handler.consumeMessage(ctx, msg));
        } catch (RejectedExecutionException e) {
            LOGGER.error("worker pool reject the task, msgId : {} .", msg.getMsgId(), e);
        }
    }

    /**
     * 客户端心跳处理
     * */
    private void doHeartbeat(ChannelHandlerContext ctx, MQMessage msg) {
        MsgHeartBeat heartBeatMsg = (MsgHeartBeat) msg.getMsg();
        LOGGER.debug("broker server receive client[] heartbeat, msgId : {} .", heartBeatMsg.getClientAddress(), msg.getMsgId());
        //
        MQMessage mqMessage = new MQMessage(MQHeartBeatAck, MQBroker, new MsgHeartBeat());
        ctx.writeAndFlush(mqMessage);
    }

    /**
     * ZkNode是否创建[/activeMQ/topic/brokerId]
     * */
    private boolean checkExist(String path) throws Exception {
        return brokerServer.getZkClientContext().isNodeExist(path);
    }

    private class MessageHandlerImpl implements MessageHandler {

        @Override
        public void storeMessage(ChannelHandlerContext ctx, MQMessage msg) {
            long msgSequence = sequence.getAndIncrement();
            MsgContent msgContent = (MsgContent) msg.getMsg();
            msgContent.setBrokerMsgSequence(msgSequence);
            msgStore.store(msgContent);
            // 消息确认
            MsgAckImpl ackMessage = new MsgAckImpl(msgContent.getTopic(), msg.getMsgId(), MsgAckStatus.SUCCESS, msgSequence, msgContent.getProducerAddress());
            MQMessage mqMessage = new MQMessage(MQMsgType.MQMessageStoreAck, MQMsgSource.MQBroker, ackMessage);
            ctx.writeAndFlush(mqMessage);
        }

        @Override
        public void consumeMessage(ChannelHandlerContext ctx, MQMessage msg) {
            MsgRequest request = (MsgRequest) msg.getMsg();
            String topic = request.getTopic();
            long start = request.getStartSequence();
            long end = request.getEndSequence();
            List<String> msgList = msgStore.getMsg(topic, start, end);

            // 响应客户端
            MsgResponse response = new MsgResponse(topic, start, end, msgList);
            MQMessage mqMessage = new MQMessage(MQMsgType.MQMessageResponse, MQMsgSource.MQBroker, response);
            ctx.writeAndFlush(mqMessage);
        }

    }

    /**
     * MQ消息处理拦截器
     * */
    private class MessageInterceptorImpl implements MessageInterceptor {

        @Override
        public void beforeProcess(Object msg) throws Exception {
            if (msg.getClass() != MsgContent.class) {
                return;
            }
            MsgContent message = (MsgContent) msg;
            if (!created.get()) {
                String brokerId = brokerServer.getBrokerId();
                String path = ZkUtils.brokerTopicNode(brokerId, message.getTopic());
                LOGGER.debug("check zk node {} .", path);
                if (!checkExist(path)) {
                    InetSocketAddress socketAddress = brokerServer.getNettyServer().getSocketAddress();
                    TopicNodeData topicNodeData = new TopicNodeData(message.getTopic(), Utils.socketAddressCastString(socketAddress), brokerId, 0);
                    String data = GsonUtils.toJson(topicNodeData);
                    String nodePath = brokerServer.getZkClientContext().createNode(path, data.getBytes());
                    if (!Strings.isNullOrEmpty(nodePath)) {
                        created.set(true);
                    }
                }
            }
        }

        @Override
        public void afterProcess(Object msg) {

        }

        @Override
        public void success(Object msg) throws Exception {
            if (msg.getClass() != MsgContent.class) {
                return;
            }
            MsgContent message = (MsgContent) msg;
            // 更新[/activeMQ/topic/topicName/brokerId]消息
            String brokeId = brokerServer.getBrokerId();
            String path = ZkUtils.brokerTopicNode(brokeId, message.getTopic());
            InetSocketAddress socketAddress = brokerServer.getNettyServer().getSocketAddress();
            TopicNodeData topicNodeData = new TopicNodeData(message.getTopic(), Utils.socketAddressCastString(socketAddress), brokeId, message.getBrokerMsgSequence());
            String data = GsonUtils.toJson(topicNodeData);
            brokerServer.getZkClientContext().updateNodeData(path, data.getBytes());
        }

        @Override
        public void failure(Object msg, Throwable t) {

        }
    }

    private class MessageInvoker implements InvocationHandler {

        private Object target;

        private MessageInterceptor interceptor;

        private String methodName;

        public MessageInvoker(Object target, MessageInterceptor interceptor, String methodName) {
            this.target = target;
            this.interceptor = interceptor;
            this.methodName = methodName;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Object msg = args[1];
            Object result = null;
            boolean invoke = false;
            try {
                String name = method.getName();
                if (name.equals(methodName)) {
                    invoke = true;
                    interceptor.beforeProcess(msg);
                    result = method.invoke(target, args);
                    interceptor.success(msg);
                    interceptor.afterProcess(msg);
                } else {
                    result = method.invoke(target, args);
                }
            } catch (Throwable t) {
                if (invoke) {
                    interceptor.failure(msg, t);
                }
            }
            return result;
        }
    }

    /**
     * Topic Zk Node数据存储格式
     */
    @Setter
    @Getter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TopicNodeData {

        // 主题
        private String topic;

        // 存储Broker Server服务地址
        private String brokerServer;

        // Broker Server UUID
        private String brokerId;

        // 当前Topic最大消息序号
        private long currentMsgSequence;

    }

}
