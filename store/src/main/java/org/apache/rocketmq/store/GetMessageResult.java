package org.apache.rocketmq.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

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

    private boolean suggestPullingFromSlave = false;

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

    public boolean isSuggestPullingFromSlave() {
        return suggestPullingFromSlave;
    }

    public void setSuggestPullingFromSlave(boolean suggestPullingFromSlave) {
        this.suggestPullingFromSlave = suggestPullingFromSlave;
    }

    public int getMsgCount4Commercial() {
        return msgCount4Commercial;
    }

    public void setMsgCount4Commercial(int msgCount4Commercial) {
        this.msgCount4Commercial = msgCount4Commercial;
    }

    @Override
    public String toString() {
        return "GetMessageResult [status=" + status + ", nextBeginOffset=" + nextBeginOffset + ", minOffset="
                + minOffset + ", maxOffset=" + maxOffset + ", bufferTotalSize=" + bufferTotalSize
                + ", suggestPullingFromSlave=" + suggestPullingFromSlave + "]";
    }

}
