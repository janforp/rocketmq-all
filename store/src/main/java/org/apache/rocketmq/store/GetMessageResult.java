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

    /**
     * 查询消息的时候，最底层都是 mappedFile 支持的查询，它查询的时候返回给外层一个 SelectMappedBufferResult 对象
     * mappedFile每查询一次都会 refCount ++ ，通过 SelectMappedBufferResult 对象中持有的 mappedFile 完成资源释放的句柄
     */
    // mappedFile.getXXX(long pos)
    private final List<SelectMappedBufferResult> messageMapedList = new ArrayList<>(100);

    // 每条消息
    private final List<ByteBuffer> messageBufferList = new ArrayList<>(100);

    @Setter
    @Getter
    private GetMessageStatus status;

    // 返回给客户端，表示下一次再向当前队列拉消息的时候的开始 offset
    @Setter
    @Getter
    private long nextBeginOffset;

    // 当前队列最小
    @Setter
    @Getter
    private long minOffset;

    // 当前队列最大
    @Setter
    @Getter
    private long maxOffset;

    // 消息总 byte 大小
    @Setter
    @Getter
    private int bufferTotalSize = 0;

    // 服务器建议客户端下一次到该队列拉取消息的时候，是从主节点还是从节点，主要根据本次查询的时候数据是冷热区别
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
