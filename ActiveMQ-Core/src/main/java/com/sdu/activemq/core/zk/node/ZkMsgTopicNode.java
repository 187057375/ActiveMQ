package com.sdu.activemq.core.zk.node;

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
public class ZkMsgTopicNode {

    // Broker Server服务地址[host:port]
    private String brokerAddress;

    // Broker Server ID
    private String brokerId;

    private String topic;

}
