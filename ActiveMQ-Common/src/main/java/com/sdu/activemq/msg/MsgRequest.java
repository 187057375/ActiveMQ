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
public class MsgRequest implements Message {

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
