package com.sdu.activemq.core.store;

import com.google.common.collect.Maps;
import com.sdu.activemq.msg.MsgStoreRequest;
import org.apache.logging.log4j.util.Strings;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author hanhan.zhang
 * */
public class MemoryMsgStore implements MsgStore<MsgStoreRequest, String> {

    // 消息存储使用TreeMap, 根据key快速获取数据
    private ConcurrentHashMap<String, SortedMap<Long, String>> msgDataSource;

    public MemoryMsgStore() {
        this.msgDataSource = new ConcurrentHashMap<>();
    }

    @Override
    public void store(MsgStoreRequest msg) {
        String topic = msg.getTopic();
        if (Strings.isEmpty(topic)) {
            return;
        }

        SortedMap<Long, String> msgData = msgDataSource.get(topic);
        if (msgData == null) {
            msgData = Collections.synchronizedSortedMap(new TreeMap<>());
        }

        String content = msg.getMsgBody();
        msgData.put(msg.getBrokerMsgSequence(), content);
        msgDataSource.put(topic, msgData);
    }

    @Override
    public Map<Long, String> getMsg(String topic, long startSequence, long endSequence) {
        SortedMap<Long, String> msgMap = msgDataSource.get(topic);

        if (msgMap == null) {
            return Collections.emptyMap();
        }

        // Kryo序列化对象必须要求有无参构造函数
        Map<Long, String> map = Maps.newHashMap();
        map.putAll(msgMap.subMap(startSequence, endSequence));

        return map;
    }
}
