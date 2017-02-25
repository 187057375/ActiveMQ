package com.sdu.activemq.core.broker;

/**
 * @author hanhan.zhang
 * */
public interface Server {

    public void start() throws Exception ;

    public void shutdown() throws Exception;

    public String getServerAddress();

}
