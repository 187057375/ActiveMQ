package com.sdu.activemq.util;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.UUID;
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

    public static String generateUUID() {
        return UUID.randomUUID().toString();
    }

    public static InetSocketAddress stringCastSocketAddress(String address, String separator) {
        if (Strings.isNullOrEmpty(address)) {
            return null;
        }
        String []items = address.split(separator);
        if (items.length < 2) {
            return null;
        }
        return new InetSocketAddress(items[0], Integer.parseInt(items[1]));
    }

    public static String socketAddressCastString(InetSocketAddress address) {
        return address.getHostString() + ":" + address.getPort();
    }
}
