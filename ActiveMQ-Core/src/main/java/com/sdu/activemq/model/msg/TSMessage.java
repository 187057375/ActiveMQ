package com.sdu.activemq.model.msg;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Arrays;
import java.util.UUID;

/**
 * @author hanhan.zhang
 * */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class TSMessage implements Message {

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

    @Override
    public String getMsgId() {
        return UUID.randomUUID().toString();
    }

}
