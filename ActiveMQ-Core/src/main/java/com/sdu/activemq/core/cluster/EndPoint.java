package com.sdu.activemq.core.cluster;

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
public class EndPoint {

    private String pointID;

    private String nodeAddress;


    public EndPoint(String nodeAddress) {
        this("", nodeAddress);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        EndPoint that = (EndPoint) object;

        return nodeAddress != null ? nodeAddress.equals(that.nodeAddress) : that.nodeAddress == null;
    }

    @Override
    public int hashCode() {
        int result = nodeAddress != null ? nodeAddress.hashCode() : 0;
        return result;
    }
}
