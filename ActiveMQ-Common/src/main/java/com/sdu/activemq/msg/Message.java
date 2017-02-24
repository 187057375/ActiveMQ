package com.sdu.activemq.msg;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
public interface Message extends Serializable {

    public String getMsgId();

}
