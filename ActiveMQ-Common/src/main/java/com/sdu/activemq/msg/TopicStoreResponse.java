package com.sdu.activemq.msg;

import com.sdu.activemq.util.Utils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Set;

/**
 * @author hanhan.zhang
 * */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class TopicStoreResponse implements Message {

    private String topic;

    private int partition;

    private Set<String> brokerAddress;

    @Override
    public String getMsgId() {
        return Utils.generateUUID();
    }

}
