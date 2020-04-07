package org.apache.rocketmq.common.message;

import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;

public class MessageExtBatch extends MessageExt {

    private static final long serialVersionUID = -2353110995348498537L;

    public ByteBuffer wrap() {
        assert getBody() != null;
        return ByteBuffer.wrap(getBody(), 0, getBody().length);
    }

    @Getter
    @Setter
    private ByteBuffer encodedBuff;
}
