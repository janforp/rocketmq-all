package org.apache.rocketmq.client.consumer;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

@Getter
@ToString
@AllArgsConstructor
public class PullResult {

    private final PullStatus pullStatus;

    // 下次拉取消息的偏移量
    private final long nextBeginOffset;

    private final long minOffset;

    private final long maxOffset;

    @Setter
    private List<MessageExt> msgFoundList;
}