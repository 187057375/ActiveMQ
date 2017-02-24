package com.sdu.activemq.model.msg;

import com.sdu.activemq.utils.Utils;
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
public class MsgSubscribe implements Message {

    private String topic;

    private String consumerGroup;

    @Override
    public String getMsgId() {
        return Utils.generateUUID();
    }
}
