package com.sdu.activemq.model.msg;

import com.sdu.activemq.utils.Utils;
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
public class MsgConsumeResponse implements Message {

    private String topic;

    private List<String> msgList;

    @Override
    public String getMsgId() {
        return Utils.generateUUID();
    }
}
