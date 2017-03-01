package com.sdu.activemq.core.store;

import java.util.Map;

/**
 * @author hanhan.zhang
 * */
public interface MsgStore<T, R> {

    public void store(T msg);

    public Map<Long, R> getMsg(String topic, long startSequence, long endSequence);

}
