package com.sdu.activemq.model.msg;

import com.sdu.activemq.utils.Utils;

/**
 * @author hanhan.zhang
 * */
public class SimpleMessage implements Message {
    @Override
    public String getMsgId() {
        return Utils.generateUUID();
    }
}
