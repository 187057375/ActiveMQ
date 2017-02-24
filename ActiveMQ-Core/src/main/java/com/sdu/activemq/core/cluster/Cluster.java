package com.sdu.activemq.core.cluster;

/**
 * @author hanhan.zhang
 * */
public interface Cluster {

    public void start() throws Exception;

    public void destroy() throws Exception ;
}
