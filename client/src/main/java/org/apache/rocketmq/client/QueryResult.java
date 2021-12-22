package org.apache.rocketmq.client;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

@AllArgsConstructor
@ToString
public class QueryResult {

    @Getter
    private final long indexLastUpdateTimestamp;

    @Getter
    private final List<MessageExt> messageList;
}
