package org.apache.rocketmq.common.protocol.header;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class QueryConsumerOffsetRequestHeader implements CommandCustomHeader {

    @CFNotNull
    @Getter
    @Setter
    private String consumerGroup;

    @CFNotNull
    @Getter
    @Setter
    private String topic;

    @CFNotNull
    @Getter
    @Setter
    private Integer queueId;

    @Override
    public void checkFields() throws RemotingCommandException {
    }
}