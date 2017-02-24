package com.sdu.activemq.msg;

import com.sdu.activemq.util.Utils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author hanhan.zhang
 * */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class MsgContent implements Message {

    // 消息主题
    private String topic;

    //
    private String producerAddress;

    // 消息体
    private byte[] msgBody;

    // 消息产生时间戳
    private long timestamp;

    // Broker Server产生的序号
    private long brokerMsgSequence;

    public MsgContent(String topic, String producerAddress, byte[] msgBody, long timestamp) {
        this(topic, producerAddress, msgBody, timestamp, 0L);
    }

    @Override
    public String getMsgId() {
        return Utils.generateUUID();
    }

}
