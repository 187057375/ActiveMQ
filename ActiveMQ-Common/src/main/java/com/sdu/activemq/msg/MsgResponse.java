package com.sdu.activemq.msg;

import com.sdu.activemq.util.Utils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

/**
 * @author hanhan.zhang
 * */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class MsgResponse implements Message {

    private String topic;

    private long start;

    private long end;

    private List<String> msgList;

    @Override
    public String getMsgId() {
        return Utils.generateUUID();
    }
}
