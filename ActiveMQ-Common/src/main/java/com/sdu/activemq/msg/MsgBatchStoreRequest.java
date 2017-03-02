package com.sdu.activemq.msg;

import com.sdu.activemq.util.Utils;
import lombok.*;

import java.util.List;

/**
 * @author hanhan.zhang
 * */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class MsgBatchStoreRequest implements Message {

    // 消息主题
    private String topic;

    //
    private String producerAddress;

    // 消息体
    private List<String> msgBody;

    // 消息产生时间戳
    private long timestamp;

    // Broker Server产生的序号
    private long brokerMsgSequence;


    @Override
    public String getMsgId() {
        return Utils.generateUUID();
    }
}
