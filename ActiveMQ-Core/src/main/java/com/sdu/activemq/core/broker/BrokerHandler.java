package com.sdu.activemq.core.broker;

import com.google.common.base.Strings;
import com.sdu.activemq.core.handler.MessageHandler;
import com.sdu.activemq.core.handler.MessageInterceptor;
import com.sdu.activemq.core.store.MemoryMsgStore;
import com.sdu.activemq.core.zk.ZkClientContext;
import com.sdu.activemq.model.MQMessage;
import com.sdu.activemq.model.MQMsgSource;
import com.sdu.activemq.model.MQMsgType;
import com.sdu.activemq.model.msg.AckMessageImpl;
import com.sdu.activemq.model.msg.HeartBeatMsg;
import com.sdu.activemq.model.msg.MsgAckStatus;
import com.sdu.activemq.model.msg.TSMessage;
import com.sdu.activemq.utils.GsonUtils;
import com.sdu.activemq.utils.Utils;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.sdu.activemq.utils.Const.ZK_TOPIC_PATH;

/**
 * Broker Server消息处理
 *
 * @author hanhan.zhang
 * */
public class BrokerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerHandler.class);

    private String brokerId;

    private InetSocketAddress brokerSocketAddress;

    private ExecutorService es;

    private MessageHandler handler;

    private ZkClientContext zkClientContext;

    private AtomicLong sequence = new AtomicLong(0L);

    private AtomicBoolean created = new AtomicBoolean(false);

    // 基于内存存储
    private MemoryMsgStore msgStore = new MemoryMsgStore();


    public BrokerHandler(ExecutorService es, String brokerId) {
        this.es = es;
        this.brokerId = brokerId;

        // 生成代理
        handler = new MessageHandlerImpl();
        MessageInterceptor interceptor = new MessageInterceptorImpl();
        MessageInvoker invoker = new MessageInvoker(handler, interceptor);
        Class<?>[] interceptorClazz = new Class[]{MessageHandlerImpl.class};
        handler = (MessageHandlerImpl) Proxy.newProxyInstance(MessageHandlerImpl.class.getClassLoader(), interceptorClazz, invoker);
    }

    public void setZkClientContext(ZkClientContext zkClientContext) {
        this.zkClientContext = zkClientContext;
    }

    public void setBrokerSocketAddress(InetSocketAddress brokerSocketAddress) {
        this.brokerSocketAddress = brokerSocketAddress;
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
            default:
                execute(ctx, mqMessage);
        }
    }

    private void execute(ChannelHandlerContext ctx, MQMessage msg) {
        try {
            es.submit(() -> handler.handleMessage(ctx, msg));
        } catch (RejectedExecutionException e) {
            LOGGER.error("worker pool reject the task, msgId : {} .", msg.getMsgId(), e);
        }
    }

    /**
     * 客户端心跳处理
     * */
    private void doHeartbeat(ChannelHandlerContext ctx, MQMessage msg) {
        HeartBeatMsg heartBeatMsg = (HeartBeatMsg) msg.getMsg();
        LOGGER.debug("broker server receive client[] heartbeat, msgId : {} .", heartBeatMsg.getClientAddress(), msg.getMsgId());
        //
        MQMessage mqMessage = new MQMessage(MQMsgType.MQHeartBeatAck, MQMsgSource.MQBroker, new HeartBeatMsg());
        ctx.writeAndFlush(mqMessage);
    }


    private String zkTopicNode(String brokerId, String topic) {
        return ZK_TOPIC_PATH + "/" + topic + "/" + brokerId;
    }

    /**
     * ZkNode是否创建[/activeMQ/topic/brokerId]
     * */
    private boolean checkExist(String path) throws Exception {
        return zkClientContext.isNodeExist(path);
    }

    private class MessageHandlerImpl implements MessageHandler {

        @Override
        public void handleMessage(ChannelHandlerContext ctx, MQMessage msg) {
            MQMsgType type = msg.getMsgType();
            switch (type) {
                case MQMessageStore:
                    doMsgStoreAndAck(ctx, msg);
                    break;
                case MQSMessageRequest:
                    doMsgRequest(ctx, msg);
                    break;
            }
        }

        private void doMsgStoreAndAck(ChannelHandlerContext ctx, MQMessage msg) {
            long msgSequence = sequence.getAndIncrement();
            TSMessage tsMessage = (TSMessage) msg.getMsg();
            tsMessage.setBrokerMsgSequence(msgSequence);
            msgStore.store(tsMessage);
            // 消息确认
            AckMessageImpl ackMessage = new AckMessageImpl(tsMessage.getTopic(), msg.getMsgId(), MsgAckStatus.SUCCESS, msgSequence, tsMessage.getProducerAddress());
            MQMessage mqMessage = new MQMessage(MQMsgType.MQMessageAck, MQMsgSource.MQBroker, ackMessage);
            ctx.writeAndFlush(mqMessage);
        }

        private void doMsgRequest(ChannelHandlerContext ctx, MQMessage msg) {

        }

    }

    /**
     * MQ消息处理拦截器
     * */
    private class MessageInterceptorImpl implements MessageInterceptor {

        @Override
        public void beforeProcess(Object msg) throws Exception {
            if (msg.getClass() != TSMessage.class) {
                return;
            }
            TSMessage message = (TSMessage) msg;
            if (!created.get()) {
                String path = zkTopicNode(brokerId, message.getTopic());
                LOGGER.debug("check zk node {} .", path);
                if (!checkExist(path)) {
                    TopicNodeData topicNodeData = new TopicNodeData(message.getTopic(), Utils.socketAddressCastString(brokerSocketAddress), brokerId, 0);
                    String data = GsonUtils.toJson(topicNodeData);
                    String nodePath = zkClientContext.createNode(path, data.getBytes());
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
            if (msg.getClass() != TSMessage.class) {
                return;
            }
            TSMessage message = (TSMessage) msg;
            // 修改ZK Topic Node下消息序列号
            String path = zkTopicNode(brokerId, message.getTopic());
            TopicNodeData topicNodeData = new TopicNodeData(message.getTopic(), Utils.socketAddressCastString(brokerSocketAddress), brokerId, 0);
            String data = GsonUtils.toJson(topicNodeData);
            zkClientContext.updateNodeData(path, data.getBytes());
        }

        @Override
        public void failure(Object msg, Throwable t) {

        }
    }

    private class MessageInvoker implements InvocationHandler {

        private Object target;

        private MessageInterceptor interceptor;

        public MessageInvoker(Object target, MessageInterceptor interceptor) {
            this.target = target;
            this.interceptor = interceptor;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Object msg = args[1];
            Object result = null;
            try {
                interceptor.beforeProcess(msg);
                result = method.invoke(target, args);
                interceptor.success(msg);
                interceptor.afterProcess(msg);
            } catch (Throwable t) {
                interceptor.failure(msg, t);
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
