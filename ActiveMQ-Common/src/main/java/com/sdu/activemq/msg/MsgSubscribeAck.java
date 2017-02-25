package com.sdu.activemq.msg;

import com.sdu.activemq.util.Utils;
import lombok.*;

/**
 * @author hanhan.zhang
 * */
@Setter
@Getter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class MsgSubscribeAck implements Message {

    private String topic;

    private MsgAckStatus status;

    @Override
    public String getMsgId() {
        return Utils.generateUUID();
    }
}
