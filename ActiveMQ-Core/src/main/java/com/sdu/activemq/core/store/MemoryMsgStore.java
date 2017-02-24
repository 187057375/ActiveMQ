package com.sdu.activemq.core.store;

import com.sdu.activemq.model.msg.TSMessage;
import org.apache.logging.log4j.util.Strings;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author hanhan.zhang
 * */
public class MemoryMsgStore implements MsgStore<TSMessage> {

    private ConcurrentHashMap<String, Map<Long, String>> msgDataSource;

    public MemoryMsgStore() {
        this.msgDataSource = new ConcurrentHashMap<>();
    }

    @Override
    public void store(TSMessage msg) {
        String topic = msg.getTopic();
        if (Strings.isEmpty(topic)) {
            return;
        }

        Map<Long, String> msgData = msgDataSource.get(topic);
        if (msgData == null) {
            msgData = new ConcurrentHashMap<>();
        }
        String content = new String(msg.getMsgBody());
        msgData.put(msg.getBrokerMsgSequence(), content);
    }
}
