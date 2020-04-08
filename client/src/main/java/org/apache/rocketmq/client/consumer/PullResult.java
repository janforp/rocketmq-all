package org.apache.rocketmq.client.consumer;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

@Getter
public class PullResult {

    private final PullStatus pullStatus;

    private final long nextBeginOffset;

    private final long minOffset;

    private final long maxOffset;

    @Setter
    private List<MessageExt> msgFoundList;

    public PullResult(PullStatus pullStatus, long nextBeginOffset, long minOffset, long maxOffset,
            List<MessageExt> msgFoundList) {
        super();
        this.pullStatus = pullStatus;
        this.nextBeginOffset = nextBeginOffset;
        this.minOffset = minOffset;
        this.maxOffset = maxOffset;
        this.msgFoundList = msgFoundList;
    }

    @Override
    public String toString() {
        return "PullResult [pullStatus=" + pullStatus + ", nextBeginOffset=" + nextBeginOffset
                + ", minOffset=" + minOffset + ", maxOffset=" + maxOffset + ", msgFoundList="
                + (msgFoundList == null ? 0 : msgFoundList.size()) + "]";
    }
}
