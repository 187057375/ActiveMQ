package com.sdu.activemq.core.handler;

/**
 * @author hanhan.zhang
 * */
public interface MessageInterceptor {

    public void beforeProcess(Object msg);

    public void afterProcess(Object msg);

    public void success(Object msg);

    public void failure(Object msg, Throwable t);

}
