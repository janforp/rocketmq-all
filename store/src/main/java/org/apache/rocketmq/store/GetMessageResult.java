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

    // mappedFile.getXXX(long pos)
    private final List<SelectMappedBufferResult> messageMapedList = new ArrayList<>(100);

    // 每条消息
    private final List<ByteBuffer> messageBufferList = new ArrayList<>(100);

    @Setter
    @Getter
    private GetMessageStatus status;

    // 下一次再拉消息的时候的开始 offset
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

    // 服务器建议下一次拉取消息的时候，是从主节点还是从节点，主要根据本次查询的时候数据是冷热区别
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
