package com.sdu.activemq.msg;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.UUID;

/**
 * @author hanhan.zhang
 * */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class MsgHeartBeat implements Message {

    private String msg;

    @Override
    public String getMsgId() {
        return UUID.randomUUID().toString();
    }
}
