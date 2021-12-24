package org.apache.rocketmq.client.impl.consumer;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class PullResultExt extends PullResult {

    /**
     * 建议下次去哪个 broker 上拉消息的 brokerId
     */
    @Getter
    private final long suggestWhichBrokerId;

    @Getter
    @Setter
    private byte[] messageBinary;

    public PullResultExt(PullStatus pullStatus, long nextBeginOffset, long minOffset, long maxOffset, List<MessageExt> msgFoundList, final long suggestWhichBrokerId, final byte[] messageBinary) {
        super(pullStatus, nextBeginOffset, minOffset, maxOffset, msgFoundList);
        this.suggestWhichBrokerId = suggestWhichBrokerId;
        this.messageBinary = messageBinary;
    }
}