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
public class ZkConsumeMetaNode {

    private String topic;

    private String topicGroup;

    private int consumeOffset;

}
