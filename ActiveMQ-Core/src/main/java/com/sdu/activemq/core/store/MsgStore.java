package com.sdu.activemq.core.store;

/**
 * @author hanhan.zhang
 * */
public interface MsgStore<T> {

    public void store(T msg);

}
