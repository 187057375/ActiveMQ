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
public class BrokerAllocateResponse implements Message {

    private String brokerId;

    private String brokerAddress;

    @Override
    public String getMsgId() {
        return Utils.generateUUID();
    }
}
