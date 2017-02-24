package com.sdu.activemq.msg;

import lombok.Getter;

import java.util.UUID;

/**
 * @author hanhan.zhang
 * */
@Getter
public class MsgHeartBeat implements Message {

    private String clientAddress;

    public MsgHeartBeat() {

    }

    public MsgHeartBeat(String clientAddress) {
        this.clientAddress = clientAddress;
    }

    @Override
    public String getMsgId() {
        return UUID.randomUUID().toString();
    }
}
