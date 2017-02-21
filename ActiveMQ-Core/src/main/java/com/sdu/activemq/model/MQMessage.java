package com.sdu.activemq.model;

import com.sdu.activemq.model.msg.Message;
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

    private int msgId;

    private MQMessageType msgType;

    private MQMessageSource msgSource;

    private Message msg;

}
