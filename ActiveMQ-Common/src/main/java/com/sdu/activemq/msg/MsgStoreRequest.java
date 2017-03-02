package com.sdu.activemq.msg;

import com.sdu.activemq.util.Utils;
import lombok.*;

/**
 * @author hanhan.zhang
 * */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class MsgStoreRequest implements Message {

    // 消息主题
    private String topic;

    //
    private String producerAddress;

    // 消息体
    private String msgBody;

    // 消息产生时间戳
    private long timestamp;

    // Broker Server产生的序号
    private long brokerMsgSequence;

    public MsgStoreRequest(String topic, String producerAddress, String msgBody, long timestamp) {
        this(topic, producerAddress, msgBody, timestamp, 0L);
    }

    @Override
    public String getMsgId() {
        return Utils.generateUUID();
    }

}
