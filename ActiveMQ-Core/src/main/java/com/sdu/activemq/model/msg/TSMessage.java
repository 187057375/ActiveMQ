package com.sdu.activemq.model.msg;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Arrays;

/**
 * @author hanhan.zhang
 * */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class TSMessage implements Message {

    // 消息唯一标识
    private int msgId;

    // 消息主题
    private String topic;

    // 消息体
    private byte[] msgBody;

    // 消息产生时间戳
    private long timestamp;

    @Override
    public int getMsgId() {
        return this.msgId;
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof TSMessage) {
            TSMessage tsMessage = (TSMessage) object;
            return tsMessage.getMsgId() == msgId && tsMessage.getTopic().equals(topic)
                    && tsMessage.getTimestamp() == timestamp;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = msgId;
        result = 31 * result + (topic != null ? topic.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(msgBody);
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }
}
