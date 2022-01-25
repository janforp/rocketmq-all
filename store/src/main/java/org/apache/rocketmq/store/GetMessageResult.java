package org.apache.rocketmq.store;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @see DefaultMessageStore#getMessage(java.lang.String, java.lang.String, int, long, int, org.apache.rocketmq.store.MessageFilter)
 */
@ToString
public class GetMessageResult {

    /**
     * 查询消息的时候，最底层都是 mappedFile 支持的查询，它查询的时候返回给外层一个 SelectMappedBufferResult 对象
     * mappedFile每查询一次都会 refCount ++ ，通过 SelectMappedBufferResult 对象中持有的 mappedFile 完成资源释放的句柄
     *
     * @see GetMessageResult#addMessage(org.apache.rocketmq.store.SelectMappedBufferResult)
     */
    @Getter
    private final List<SelectMappedBufferResult> messageMappedList = new ArrayList<>(100);

    // 每条消息

    /**
     * @see GetMessageResult#addMessage(org.apache.rocketmq.store.SelectMappedBufferResult)
     */
    @Getter
    private final List<ByteBuffer/* 存储一条消息 */> messageBufferList = new ArrayList<>(100);

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

    /**
     * @see GetMessageResult#addMessage(org.apache.rocketmq.store.SelectMappedBufferResult)
     */
    @Setter
    @Getter
    private int bufferTotalSize = 0;

    // 服务器建议客户端下一次到该队列拉取消息的时候，是从主节点还是从节点，主要根据本次查询的时候数据是冷热区别
    @Getter
    @Setter
    private boolean suggestPullingFromSlave = false;

    /**
     * @see GetMessageResult#addMessage(org.apache.rocketmq.store.SelectMappedBufferResult)
     */
    @Getter
    @Setter
    private int msgCount4Commercial = 0;

    public GetMessageResult() {
    }

    public void addMessage(final SelectMappedBufferResult mappedBuffer) {
        this.messageMappedList.add(mappedBuffer);
        this.messageBufferList.add(mappedBuffer.getByteBuffer());
        this.bufferTotalSize += mappedBuffer.getSize();
        this.msgCount4Commercial += (int) Math.ceil(mappedBuffer.getSize() / BrokerStatsManager.SIZE_PER_COUNT);
    }

    public void release() {
        for (SelectMappedBufferResult select : this.messageMappedList) {
            select.release();
        }
    }

    public int getMessageCount() {
        return this.messageMappedList.size();
    }
}
