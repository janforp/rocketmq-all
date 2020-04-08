package org.apache.rocketmq.client;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

@AllArgsConstructor
public class QueryResult {

    @Getter
    private final long indexLastUpdateTimestamp;

    @Getter
    private final List<MessageExt> messageList;

    @Override
    public String toString() {
        return "QueryResult [indexLastUpdateTimestamp=" + indexLastUpdateTimestamp + ", messageList="
                + messageList + "]";
    }
}
