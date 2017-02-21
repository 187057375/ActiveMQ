package com.sdu.activemq.model.msg;

import lombok.AllArgsConstructor;
import lombok.NonNull;

/**
 * @author hanhan.zhang
 * */
@AllArgsConstructor
public class AckMessageImpl implements AckMessage {

    @NonNull
    private int msgId;

    @NonNull
    private MsgAckStatus status;

    @Override
    public MsgAckStatus getAckStatus() {
        return status;
    }

    @Override
    public int getMsgId() {
        return msgId;
    }
}
