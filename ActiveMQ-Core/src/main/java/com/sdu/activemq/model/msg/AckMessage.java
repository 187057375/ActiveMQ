package com.sdu.activemq.model.msg;

/**
 * @author hanhan.zhang
 * */
public interface AckMessage extends Message {

    public MsgAckStatus getAckStatus();

}
