package org.apache.rocketmq.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

@ToString
public class GetMessageResult {

    private final List<SelectMappedBufferResult> messageMapedList = new ArrayList<SelectMappedBufferResult>(100);

    private final List<ByteBuffer> messageBufferList = new ArrayList<ByteBuffer>(100);

    @Setter
    @Getter
    private GetMessageStatus status;

    @Setter
    @Getter
    private long nextBeginOffset;

    @Setter
    @Getter
    private long minOffset;

    @Setter
    @Getter
    private long maxOffset;

    @Setter
    @Getter
    private int bufferTotalSize = 0;

    @Getter
    @Setter
    private boolean suggestPullingFromSlave = false;

    @Getter
    @Setter
    private int msgCount4Commercial = 0;

    public GetMessageResult() {
    }

    public List<SelectMappedBufferResult> getMessageMapedList() {
        return messageMapedList;
    }

    public List<ByteBuffer> getMessageBufferList() {
        return messageBufferList;
    }

    public void addMessage(final SelectMappedBufferResult mapedBuffer) {
        this.messageMapedList.add(mapedBuffer);
        this.messageBufferList.add(mapedBuffer.getByteBuffer());
        this.bufferTotalSize += mapedBuffer.getSize();
        this.msgCount4Commercial += (int) Math.ceil(mapedBuffer.getSize() / BrokerStatsManager.SIZE_PER_COUNT);
    }

    public void release() {
        for (SelectMappedBufferResult select : this.messageMapedList) {
            select.release();
        }
    }

    public int getMessageCount() {
        return this.messageMapedList.size();
    }
}
