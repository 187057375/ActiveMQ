package com.sdu.activemq.msg;

/**
 * @author hanhan.zhang
 * */
public interface MsgAck extends Message {

    public MsgAckStatus getAckStatus();

}
