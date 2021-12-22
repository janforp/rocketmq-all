package org.apache.rocketmq.store;

import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class QueryMessageResult {

    private final List<SelectMappedBufferResult> messageMapedList = new ArrayList<>(100);

    @Getter
    private final List<ByteBuffer> messageBufferList = new ArrayList<ByteBuffer>(100);

    @Getter
    @Setter
    private long indexLastUpdateTimestamp;

    @Getter
    @Setter
    private long indexLastUpdatePhyoffset;

    @Getter
    private int bufferTotalSize = 0;

    public void addMessage(final SelectMappedBufferResult mapedBuffer) {
        this.messageMapedList.add(mapedBuffer);
        this.messageBufferList.add(mapedBuffer.getByteBuffer());
        this.bufferTotalSize += mapedBuffer.getSize();
    }

    public void release() {
        for (SelectMappedBufferResult select : this.messageMapedList) {
            select.release();
        }
    }
}
