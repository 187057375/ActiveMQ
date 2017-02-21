package com.sdu.activemq.core.broker;

import com.sdu.activemq.core.handler.MessageHandler;
import com.sdu.activemq.core.handler.MessageInterceptor;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * @author hanhan.zhang
 * */
public class BrokerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerHandler.class);

    private ExecutorService es;

    private MessageHandler handler;

    public BrokerHandler(ExecutorService es) {
        this.es = es;

        // 生成代理
        handler = new MessageHandlerImpl();
        MessageInterceptor interceptor = new MessageInterceptorImpl();
        MessageInvoker invoker = new MessageInvoker(handler, interceptor);
        Class<?>[] interceptorClazz = new Class[]{MessageHandlerImpl.class};
        handler = (MessageHandlerImpl) Proxy.newProxyInstance(MessageHandlerImpl.class.getClassLoader(), interceptorClazz, invoker);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        try {
            es.submit(() -> handler.handleMessage(ctx, msg));
        } catch (RejectedExecutionException e) {
            LOGGER.error("message process worker thread pool reject the task .", e);
        }
    }

    private class MessageHandlerImpl implements MessageHandler {

        @Override
        public void handleMessage(ChannelHandlerContext ctx, Object msg) {

        }
    }

    /**
     * MQ消息处理拦截器
     * */
    private class MessageInterceptorImpl implements MessageInterceptor {

        @Override
        public void beforeProcess(Object msg) {

        }

        @Override
        public void afterProcess(Object msg) {

        }

        @Override
        public void success(Object msg) {

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

}
