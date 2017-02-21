package com.sdu.activemq.network.utils;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ThreadFactory;

/**
 *
 * Netty工具类
 *
 * @author hanhan.zhang
 * */
public class NettyUtils {

    public static ThreadFactory buildThreadFactory(String format, boolean daemon) {
        return new ThreadFactoryBuilder().setNameFormat(format).setDaemon(daemon).build();
    }

    public static EventLoopGroup createEventLoopGroup(boolean ePoll, int threadNum, String format) {
        return createEventLoopGroup(ePoll, threadNum, format, false);
    }

    public static EventLoopGroup createEventLoopGroup(boolean ePoll, int threadNum, String format, boolean daemon) {
        if (ePoll) {
            return createEventLoopGroup(ePoll, threadNum, buildThreadFactory(format, daemon));
        }
        return createEventLoopGroup(ePoll, threadNum, buildThreadFactory(format, daemon));
    }

    public static EventLoopGroup createEventLoopGroup(boolean ePoll, int threadNum, ThreadFactory threadFactory) {
        if (ePoll) {
            return new EpollEventLoopGroup(threadNum, threadFactory);
        }
        return new NioEventLoopGroup(threadNum, threadFactory);
    }

    public static Class<? extends SocketChannel> getClientChannelClass(boolean ePoll) {
        if (ePoll) {
            return EpollSocketChannel.class;
        }
        return NioSocketChannel.class;
    }

    public static Class<? extends ServerChannel> getServerChannelClass(boolean ePoll) {
        if (ePoll) {
            return EpollServerSocketChannel.class;
        }
        return NioServerSocketChannel.class;
    }

    public static final InetSocketAddress getInetSocketAddress(String address) {
        if (Strings.isNullOrEmpty(address)) {
            throw new IllegalStateException("address is illegal");
        }
        String []items = address.split(":");
        return new InetSocketAddress(items[0], Integer.parseInt(items[1]));
    }

}
