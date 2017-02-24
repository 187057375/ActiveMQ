package com.sdu.activemq.core.store;

import java.util.List;

/**
 * @author hanhan.zhang
 * */
public interface MsgStore<T, R> {

    public void store(T msg);

    public List<R> getMsg(String topic, long startSequence, long endSequence);

}
