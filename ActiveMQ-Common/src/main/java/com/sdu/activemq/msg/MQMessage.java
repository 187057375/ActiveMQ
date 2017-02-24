package com.sdu.activemq.msg;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 *
 *
 * @author hanhan.zhang
 * */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class MQMessage {

    private String msgId;

    private MQMsgType msgType;

    private MQMsgSource msgSource;

    private Message msg;

    public MQMessage(MQMsgType type, MQMsgSource source, Message msg) {
        this(msg.getMsgId(), type, source, msg);
    }

}
