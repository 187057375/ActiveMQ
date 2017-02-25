package com.sdu.activemq.msg;

import lombok.*;

/**
 * @author hanhan.zhang
 * */
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class MsgAckImpl implements MsgAck {

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
