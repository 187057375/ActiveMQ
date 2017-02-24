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
public class MsgSubscribeAck implements Message {

    private String topic;

    private MsgAckStatus status;

    @Override
    public String getMsgId() {
        return Utils.generateUUID();
    }
}
