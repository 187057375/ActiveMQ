package com.sdu.activemq.core.cluster;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.net.InetSocketAddress;

/**
 * @author hanhan.zhang
 * */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class BrokerNode {

    private String brokerUUID;

    private InetSocketAddress socketAddress;

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        BrokerNode that = (BrokerNode) object;

        return socketAddress != null ? socketAddress.equals(that.socketAddress) : that.socketAddress == null;
    }

    @Override
    public int hashCode() {
        int result = socketAddress != null ? socketAddress.hashCode() : 0;
        return result;
    }
}
