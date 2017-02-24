package com.sdu.activemq.model.msg;

import lombok.Getter;

import java.util.UUID;

/**
 * @author hanhan.zhang
 * */
@Getter
public class HeartBeatMsg implements Message {

    private String clientAddress;

    public HeartBeatMsg() {

    }

    public HeartBeatMsg(String clientAddress) {
        this.clientAddress = clientAddress;
    }

    @Override
    public String getMsgId() {
        return UUID.randomUUID().toString();
    }
}
