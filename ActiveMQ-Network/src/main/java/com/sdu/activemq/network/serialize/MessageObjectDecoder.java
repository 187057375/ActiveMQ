package com.sdu.activemq.network.serialize;

import com.sdu.activemq.network.serialize.kryo.KryoSerializer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * @author hanhan.zhang
 * */
public class MessageObjectDecoder extends ByteToMessageDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageObjectDecoder.class);

    private final static int MESSAGE_LENGTH = 4;

    private KryoSerializer serialize;

    public MessageObjectDecoder(KryoSerializer serialize) {
        this.serialize = serialize;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // 读取包头长度
        if (in.readableBytes() < MessageObjectDecoder.MESSAGE_LENGTH) {
            return;
        }

        // 标记Buf可读位置
        in.markReaderIndex();
        int messageLength = in.readInt();

        if (messageLength < 0) {
            ctx.close();
        }

        if (in.readableBytes() < messageLength) {
            // 可读字节长度小于实际包体长度, 则发生读半包, 重置Buf的readerIndex为markedReaderIndex
            in.resetReaderIndex();
        } else {
            byte[] messageBody = new byte[messageLength];
            in.readBytes(messageBody);
            try {
                Object obj = serialize.decode(messageBody);
                out.add(obj);
            } catch (IOException ex) {
                LOGGER.error("socket bytes serialize to object exception", ex);
            }
        }
    }

    @Override
    public boolean isSharable() {
        return false;
    }
}
