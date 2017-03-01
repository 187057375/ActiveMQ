package com.sdu.activemq.core.store;

import com.sdu.activemq.msg.MsgContent;
import org.apache.logging.log4j.util.Strings;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author hanhan.zhang
 * */
public class MemoryMsgStore implements MsgStore<MsgContent, String> {

    // 消息存储使用TreeMap, 根据key快速获取数据
    private ConcurrentHashMap<String, SortedMap<Long, String>> msgDataSource;

    public MemoryMsgStore() {
        this.msgDataSource = new ConcurrentHashMap<>();
    }

    @Override
    public void store(MsgContent msg) {
        String topic = msg.getTopic();
        if (Strings.isEmpty(topic)) {
            return;
        }

        SortedMap<Long, String> msgData = msgDataSource.get(topic);
        if (msgData == null) {
            msgData = Collections.synchronizedSortedMap(new TreeMap<>());
        }

        String content = new String(msg.getMsgBody());
        msgData.put(msg.getBrokerMsgSequence(), content);
        msgDataSource.put(topic, msgData);
    }

    @Override
    public Map<Long, String> getMsg(String topic, long startSequence, long endSequence) {
        SortedMap<Long, String> msgMap = msgDataSource.get(topic);

        if (msgMap == null) {
            return Collections.emptyMap();
        }

        return msgMap.subMap(startSequence, endSequence);
    }
}
