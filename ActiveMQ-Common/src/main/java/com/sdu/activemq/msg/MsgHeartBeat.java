package com.sdu.activemq.msg;

import lombok.*;

import java.util.UUID;

/**
 * @author hanhan.zhang
 * */
@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class MsgHeartBeat implements Message {

    private String msg;

    @Override
    public String getMsgId() {
        return UUID.randomUUID().toString();
    }
}
