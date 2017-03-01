package com.sdu.activemq.core.disruptor;

/**
 *
 * @author hanhan.zhang
 * */
public interface MessageHandler {

    public void handle(Object msg) throws Exception ;

}
