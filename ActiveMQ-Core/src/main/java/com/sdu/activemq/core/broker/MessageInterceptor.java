package com.sdu.activemq.core.broker;

/**
 * @author hanhan.zhang
 * */
public interface MessageInterceptor {

    public void beforeProcess(Object msg) throws Exception;

    public void afterProcess(Object msg) throws Exception;

    public void success(Object msg) throws Exception;

    public void failure(Object msg, Throwable t) throws Exception;

}
