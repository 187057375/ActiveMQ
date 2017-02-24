package com.sdu.activemq.core.store;

import com.google.common.collect.Lists;
import com.sdu.activemq.model.msg.TSMessage;
import org.apache.logging.log4j.util.Strings;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author hanhan.zhang
 * */
public class MemoryMsgStore implements MsgStore<TSMessage, String> {

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

    @Override
    public List<String> getMsg(String topic, long startSequence, long endSequence) {
        Map<Long, String> msgMap = msgDataSource.get(topic);

        if (msgMap == null) {
            return Collections.emptyList();
        }

        List<String> msgList = Lists.newLinkedList();
        msgMap.forEach((sequence, msg) -> {
            if (sequence >= startSequence && sequence <= endSequence) {
                msgList.add(msg);
            }
        });

        return msgList;
    }
}
