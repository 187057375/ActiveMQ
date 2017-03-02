package com.sdu.activemq.core.disruptor;

import java.util.List;

/**
 *
 * @author hanhan.zhang
 * */
public interface MessageHandler {

    public void handle(Object msg) throws Exception ;

    public void handle(List<Object> batchMsg) throws Exception;

}
