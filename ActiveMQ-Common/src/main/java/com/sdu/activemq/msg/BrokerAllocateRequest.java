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
@NoArgsConstructor
@AllArgsConstructor
public class BrokerAllocateRequest implements Message {

    private String topic;

    private int partition;

    @Override
    public String getMsgId() {
        return Utils.generateUUID();
    }

}
