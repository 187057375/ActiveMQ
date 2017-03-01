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
public class MsgConsumeRequest implements Message {

    private String topic;

    // 起始序号
    private long startSequence;

    // 终止序号
    private long endSequence;

    @Override
    public String getMsgId() {
        return Utils.generateUUID();
    }
}
