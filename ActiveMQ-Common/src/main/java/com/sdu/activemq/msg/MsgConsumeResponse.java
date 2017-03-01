package com.sdu.activemq.msg;

import com.sdu.activemq.util.Utils;
import lombok.*;

import java.util.List;
import java.util.Map;

/**
 * @author hanhan.zhang
 * */
@Setter
@Getter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class MsgConsumeResponse implements Message {

    private String topic;

    private Map<Long, String> messages;

    @Override
    public String getMsgId() {
        return Utils.generateUUID();
    }
}
