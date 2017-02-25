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
public class MsgSubscribe implements Message {

    private String topic;

    private String consumerGroup;

    @Override
    public String getMsgId() {
        return Utils.generateUUID();
    }
}
