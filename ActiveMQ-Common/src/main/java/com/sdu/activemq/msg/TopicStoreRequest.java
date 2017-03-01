package com.sdu.activemq.msg;

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
public class TopicStoreRequest implements Message {

    private String topic;

    private int partition;

    @Override
    public String getMsgId() {
        return null;
    }
}
