package com.sdu.activemq.network.serialize;

import com.sdu.activemq.network.serialize.kryo.KryoSerializer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 *
 * @author hanhan.zhang
 * */
public class MessageObjectEncoder extends MessageToByteEncoder<Object> {

    private KryoSerializer serialize;

    public MessageObjectEncoder(KryoSerializer serialize) {
        this.serialize = serialize;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        serialize.encode(out, msg);
    }
}
