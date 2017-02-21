package com.sdu.activemq.utils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ThreadFactory;

/**
 *
 * @author hanhan.zhang
 * */
public class Utils {

    public static String getIpV4() {
        try {
            InetAddress address  = InetAddress.getLocalHost();
            return address.getHostAddress();
        } catch (UnknownHostException e) {
            // ignore
        }
        return "127.0.0.1";
    }

    public static ThreadFactory buildThreadFactory(String format) {
        return buildThreadFactory(format, Thread.NORM_PRIORITY, false);
    }

    public static ThreadFactory buildThreadFactory(String format, int priority, boolean daemon) {
        return new ThreadFactoryBuilder().setNameFormat(format)
                                         .setDaemon(daemon)
                                         .setPriority(priority)
                                         .build();
    }

}
