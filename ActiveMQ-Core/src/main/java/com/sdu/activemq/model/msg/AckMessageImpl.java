package com.sdu.activemq.model.msg;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

/**
 * @author hanhan.zhang
 * */
@AllArgsConstructor
public class AckMessageImpl implements AckMessage {

    @Getter
    private String topic;

    @NonNull
    private String msgId;

    @NonNull
    private MsgAckStatus status;

    @Getter
    private long brokerMsgSequence;

    @Getter
    private String producerAddress;

    @Override
    public MsgAckStatus getAckStatus() {
        return status;
    }

    @Override
    public String getMsgId() {
        return msgId;
    }
}
