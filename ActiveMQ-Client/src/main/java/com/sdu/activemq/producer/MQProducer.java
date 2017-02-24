package com.sdu.activemq.producer;

import com.google.common.collect.Maps;
import com.sdu.activemq.msg.MQMessage;
import com.sdu.activemq.msg.MsgAckImpl;
import com.sdu.activemq.msg.MsgContent;
import com.sdu.activemq.network.client.NettyClient;
import com.sdu.activemq.network.client.NettyClientConfig;
import com.sdu.activemq.network.serialize.MessageObjectDecoder;
import com.sdu.activemq.network.serialize.MessageObjectEncoder;
import com.sdu.activemq.network.serialize.kryo.KryoSerializer;
import com.sdu.activemq.util.Utils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.sdu.activemq.msg.MQMsgSource.ActiveMQProducer;
import static com.sdu.activemq.msg.MQMsgType.MQMessageStore;
import static com.sdu.activemq.msg.MQMsgType.MQMessageStoreAck;

/**
 *
 * @author hanhan.zhang
 * */
public class MQProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MQProducer.class);

    private String clusterAddress;

    private NettyClient nettyClient;

    public MQProducer(String clusterAddress) {
        this.clusterAddress = clusterAddress;
    }

    public void start() {
        NettyClientConfig clientConfig = new NettyClientConfig();
        clientConfig.setEPool(false);
        clientConfig.setSocketThreads(10);
        clientConfig.setClientThreadFactory(Utils.buildThreadFactory("producer-socket-thread-%d"));
        clientConfig.setRemoteAddress(clusterAddress);

        // Channel
        Map<ChannelOption, Object> options = Maps.newHashMap();
        options.put(ChannelOption.SO_SNDBUF, 1024);
        options.put(ChannelOption.SO_RCVBUF, 1024);
        options.put(ChannelOption.TCP_NODELAY, true);
        options.put(ChannelOption.SO_KEEPALIVE, false);
        clientConfig.setOptions(options);

        clientConfig.setChannelHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                KryoSerializer serializer = new KryoSerializer(MQMessage.class);
                ch.pipeline().addLast(new MessageObjectDecoder(serializer));
                ch.pipeline().addLast(new MessageObjectEncoder(serializer));
                ch.pipeline().addLast(new ProducerMsgHandler());
            }
        });

        // 创建Broker Server's Client并启动
        nettyClient = new NettyClient(clientConfig);
        nettyClient.start();

        if (nettyClient.isStarted()) {
            LOGGER.info("transport connect cluster[{}] success .", clusterAddress);
        }
    }

    private class ProducerMsgHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            ctx.channel().eventLoop().scheduleAtFixedRate(() -> {
                String address = Utils.socketAddressCastString(nettyClient.getLocalSocketAddress());
                String content = "MQ msg content : " + Utils.generateUUID();
                MsgContent msgContent = new MsgContent("MQTest", address, content.getBytes(), System.currentTimeMillis());
                MQMessage msg = new MQMessage(MQMessageStore, ActiveMQProducer, msgContent);
                ctx.writeAndFlush(msg);
            }, 5, 5, TimeUnit.SECONDS);

            super.channelActive(ctx);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg.getClass() == MQMessage.class) {
                MQMessage mqMessage = (MQMessage) msg;
                if (mqMessage.getMsgType() == MQMessageStoreAck) {
                    MsgAckImpl msgAck = (MsgAckImpl) mqMessage.getMsg();
                    LOGGER.info("MQ msg store ack : {}", msgAck);
                }
            }
            super.channelRead(ctx, msg);
        }
    }

    public static void main(String[] args) {
        String clusterAddress = "127.0.0.1:6712";
        MQProducer producer = new MQProducer(clusterAddress);
        producer.start();
    }
}
